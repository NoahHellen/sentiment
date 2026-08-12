"""Per-IP rate limiting.

In-process and deliberately simple: a sliding window of request timestamps per
client. That is honest about its limits — the counters live in this process, so
they reset on restart and each App Service instance enforces its own budget. For
a public demo service that is the right trade; anything stricter needs shared
state (Redis) or the platform's own gateway.
"""

import time
from collections import defaultdict, deque

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})


class SlidingWindow:
    def __init__(self, limit: int, window: float) -> None:
        self._limit = limit
        self._window = window
        self._hits: dict[str, deque[float]] = defaultdict(deque)

    def hit(self, key: str) -> float | None:
        """Record a request. Returns None if allowed, else seconds to wait."""
        now = time.monotonic()
        cutoff = now - self._window
        timestamps = self._hits[key]
        while timestamps and timestamps[0] <= cutoff:
            timestamps.popleft()

        if len(timestamps) >= self._limit:
            # The oldest request in the window is the one that has to age out.
            return timestamps[0] - cutoff

        timestamps.append(now)
        return None

    def prune(self) -> None:
        """Drop keys with no recent activity, so one-off clients don't leak."""
        cutoff = time.monotonic() - self._window
        for key in [k for k, v in self._hits.items() if not v or v[-1] <= cutoff]:
            del self._hits[key]


def client_ip(request: Request) -> str:
    """Behind App Service the peer address is the platform's proxy, so the real
    client is the first entry of X-Forwarded-For. Spoofable in general, which is
    why this is throttling rather than a security control."""
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


class RateLimitMiddleware(BaseHTTPMiddleware):
    def __init__(
        self,
        app,
        *,
        writes_per_window: int,
        reads_per_window: int,
        window_seconds: float,
        exempt_paths: tuple[str, ...] = ("/health", "/health/db", "/docs", "/openapi.json"),
    ) -> None:
        super().__init__(app)
        self._writes = SlidingWindow(writes_per_window, window_seconds)
        self._reads = SlidingWindow(reads_per_window, window_seconds)
        self._exempt = exempt_paths
        self._window = window_seconds
        self._last_prune = time.monotonic()

    async def dispatch(self, request: Request, call_next):
        if request.url.path in self._exempt:
            return await call_next(request)

        now = time.monotonic()
        if now - self._last_prune > self._window:
            self._writes.prune()
            self._reads.prune()
            self._last_prune = now

        bucket = self._reads if request.method in SAFE_METHODS else self._writes
        retry_after = bucket.hit(client_ip(request))
        if retry_after is not None:
            # Same shape as the upstream 429s the fetcher itself handles: a
            # Retry-After a client can actually act on.
            return JSONResponse(
                status_code=429,
                content={"detail": "rate limit exceeded", "retry_after": round(retry_after, 1)},
                headers={"Retry-After": str(max(1, int(retry_after) + 1))},
            )

        return await call_next(request)
