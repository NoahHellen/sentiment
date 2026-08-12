"""HTTP fetching and text extraction.

Everything that can go wrong with an unreliable network is classified here into
a transient or permanent `ItemFailure`, so the worker loop stays a plain
state machine and never has to interpret an httpx exception.
"""

import asyncio
import hashlib
import ipaddress
import socket
from dataclasses import dataclass

import httpx
from bs4 import BeautifulSoup

from services.errors import ErrorKind, parse_retry_after, permanent, transient
from services.settings import WorkerSettings

# Elements whose text is navigation or markup noise rather than page content.
_STRIP_TAGS = ("script", "style", "noscript", "template", "nav", "header", "footer", "aside", "form")
_TEXTUAL_TYPES = ("text/html", "text/plain", "application/xhtml+xml", "application/xml", "text/xml")


@dataclass(frozen=True, slots=True)
class FetchResult:
    http_status: int
    text: str
    content_hash: str


class Fetcher:
    def __init__(self, settings: WorkerSettings) -> None:
        self._settings = settings
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(
                settings.total_timeout_seconds,
                connect=settings.connect_timeout_seconds,
                read=settings.read_timeout_seconds,
            ),
            limits=httpx.Limits(
                max_connections=settings.fetch_concurrency,
                max_keepalive_connections=settings.fetch_concurrency,
            ),
            follow_redirects=True,
            max_redirects=settings.max_redirects,
            headers={"User-Agent": settings.user_agent, "Accept-Encoding": "gzip, deflate"},
        )
        # One semaphore per host: the global limit stops us melting our own
        # egress, this stops us melting somebody else's site.
        self._host_semaphores: dict[str, asyncio.Semaphore] = {}

    async def aclose(self) -> None:
        await self._client.aclose()

    def _host_semaphore(self, host: str) -> asyncio.Semaphore:
        semaphore = self._host_semaphores.get(host)
        if semaphore is None:
            semaphore = asyncio.Semaphore(self._settings.per_host_concurrency)
            self._host_semaphores[host] = semaphore
        return semaphore

    async def fetch(self, url: str) -> FetchResult:
        host = httpx.URL(url).host
        if not host:
            raise permanent(ErrorKind.INVALID_URL, url)

        await self._assert_public(host)

        async with self._host_semaphore(host):
            try:
                async with self._client.stream("GET", url) as response:
                    # A redirect can walk us onto a private address even when the
                    # submitted host was public, so the landing host is checked too.
                    if response.url.host and response.url.host != host:
                        await self._assert_public(response.url.host)

                    self._raise_for_status(response)
                    self._raise_for_content_type(response)
                    body = await self._read_capped(response)
                    encoding = response.charset_encoding or "utf-8"
            except httpx.TooManyRedirects as exc:
                raise permanent(ErrorKind.TOO_MANY_REDIRECTS, str(exc)) from exc
            except httpx.TimeoutException as exc:
                raise transient(ErrorKind.TIMEOUT, str(exc)) from exc
            except httpx.TransportError as exc:
                # Connection reset, DNS blip, TLS handshake failure: all things
                # that routinely succeed on a second attempt.
                raise transient(ErrorKind.CONNECTION, str(exc)) from exc

        text = extract_text(body.decode(encoding, errors="replace"), self._settings.max_text_chars)
        if not text:
            raise permanent(ErrorKind.EMPTY_CONTENT, "no extractable text")

        return FetchResult(
            http_status=response.status_code,
            text=text,
            content_hash=hashlib.sha256(text.encode("utf-8")).hexdigest(),
        )

    def _raise_for_status(self, response: httpx.Response) -> None:
        status = response.status_code
        if status < 400:
            return
        retry_after = parse_retry_after(response.headers.get("Retry-After"))
        if status == 429:
            raise transient(ErrorKind.TOO_MANY_REQUESTS, "rate limited", retry_after=retry_after)
        if status == 408:
            raise transient(ErrorKind.TIMEOUT, "request timeout")
        if status >= 500:
            raise transient(ErrorKind.HTTP_SERVER_ERROR, f"HTTP {status}", retry_after=retry_after)
        # 404, 401, 403, 410: retrying an identical request gets an identical
        # answer, so burning four more attempts on it only delays the batch.
        raise permanent(ErrorKind.HTTP_CLIENT_ERROR, f"HTTP {status}")

    def _raise_for_content_type(self, response: httpx.Response) -> None:
        content_type = response.headers.get("Content-Type", "").split(";")[0].strip().lower()
        if content_type and not any(content_type.startswith(t) for t in _TEXTUAL_TYPES):
            raise permanent(ErrorKind.NOT_TEXT, content_type)

        # Trust Content-Length when it is present: cheaper to reject a 2GB file
        # before reading a byte of it.
        declared = response.headers.get("Content-Length")
        if declared and declared.isdigit() and int(declared) > self._settings.max_response_bytes:
            raise permanent(ErrorKind.RESPONSE_TOO_LARGE, f"{declared} bytes declared")

    async def _read_capped(self, response: httpx.Response) -> bytes:
        """Stream the body, aborting past the cap.

        Streaming rather than `response.read()` means a hostile or misconfigured
        server can't exhaust memory: we stop pulling as soon as we've seen enough.
        """
        chunks: list[bytes] = []
        total = 0
        async for chunk in response.aiter_bytes():
            total += len(chunk)
            if total > self._settings.max_response_bytes:
                raise permanent(ErrorKind.RESPONSE_TOO_LARGE, f"over {total} bytes")
            chunks.append(chunk)
        return b"".join(chunks)

    async def _assert_public(self, host: str) -> None:
        """Reject hosts that resolve to internal addresses.

        Without this, any client could make the service fetch 169.254.169.254 or
        localhost on its behalf. This resolves and checks every address, though
        it is inherently best-effort — DNS can change between this check and the
        connection.
        """
        if self._settings.allow_private_hosts:
            return
        loop = asyncio.get_running_loop()
        try:
            infos = await loop.getaddrinfo(host, None, proto=socket.IPPROTO_TCP)
        except socket.gaierror as exc:
            raise transient(ErrorKind.CONNECTION, f"dns: {exc}") from exc

        for info in infos:
            address = ipaddress.ip_address(info[4][0])
            if (
                address.is_private
                or address.is_loopback
                or address.is_link_local
                or address.is_reserved
                or address.is_multicast
                or address.is_unspecified
            ):
                raise permanent(ErrorKind.BLOCKED_HOST, f"{host} resolves to {address}")


def extract_text(html: str, max_chars: int) -> str:
    """Reduce a page to readable text.

    Deliberately simple. A real deployment would want something like trafilatura
    for boilerplate removal, but that is a content-quality problem, not the
    pipeline problem this service is about.
    """
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(_STRIP_TAGS):
        tag.decompose()
    text = " ".join(soup.get_text(" ", strip=True).split())
    return text[:max_chars]
