"""URL normalisation, so that trivially different spellings of the same page
collapse to one item.

Lives here rather than in the fetcher because dedupe has to happen at submit
time — the unique constraint on (batch_id, url_hash) is what enforces it.
"""

import hashlib
from urllib.parse import urlsplit, urlunsplit

_DEFAULT_PORTS = {"http": 80, "https": 443}


def normalise(url: str) -> str:
    """Lowercase the scheme and host, drop a default port and any fragment.

    Deliberately conservative: query strings and trailing slashes are preserved,
    since both can change what a server returns.
    """
    parts = urlsplit(url.strip())
    scheme = parts.scheme.lower()
    host = (parts.hostname or "").lower()

    netloc = host
    if parts.port is not None and parts.port != _DEFAULT_PORTS.get(scheme):
        netloc = f"{host}:{parts.port}"
    if parts.username:
        credentials = parts.username
        if parts.password:
            credentials = f"{credentials}:{parts.password}"
        netloc = f"{credentials}@{netloc}"

    return urlunsplit((scheme, netloc, parts.path, parts.query, ""))


def fingerprint(normalised_url: str) -> str:
    """Stable 64-char hash used as the dedupe key."""
    return hashlib.sha256(normalised_url.encode("utf-8")).hexdigest()
