"""Failure classification and backoff.

The single most important decision the pipeline makes about a failed item is
*whether it is worth trying again*. Retrying a 404 wastes attempts and delays the
batch; giving up on a connection reset loses work that would have succeeded.
So failures carry that decision explicitly rather than being inferred at the
call site.
"""

import random
from email.utils import parsedate_to_datetime
from datetime import UTC, datetime
from enum import StrEnum


class ErrorKind(StrEnum):
    TIMEOUT = "timeout"
    CONNECTION = "connection"
    TOO_MANY_REQUESTS = "too_many_requests"
    HTTP_SERVER_ERROR = "http_5xx"
    HTTP_CLIENT_ERROR = "http_4xx"
    TOO_MANY_REDIRECTS = "too_many_redirects"
    RESPONSE_TOO_LARGE = "response_too_large"
    NOT_TEXT = "not_text"
    EMPTY_CONTENT = "empty_content"
    BLOCKED_HOST = "blocked_host"
    INVALID_URL = "invalid_url"
    LLM_ERROR = "llm_error"
    LLM_RATE_LIMITED = "llm_rate_limited"
    UNKNOWN = "unknown"


class ItemFailure(Exception):
    """A failure attributable to one item.

    `transient` decides retry vs. give-up. `retry_after` lets a server that told
    us when to come back (429 / 503 with Retry-After) override our own backoff.
    """

    def __init__(
        self,
        kind: ErrorKind,
        detail: str = "",
        *,
        transient: bool,
        retry_after: float | None = None,
    ) -> None:
        super().__init__(f"{kind}: {detail}" if detail else str(kind))
        self.kind = kind
        self.detail = detail[:1000]
        self.transient = transient
        self.retry_after = retry_after


def transient(kind: ErrorKind, detail: str = "", retry_after: float | None = None) -> ItemFailure:
    return ItemFailure(kind, detail, transient=True, retry_after=retry_after)


def permanent(kind: ErrorKind, detail: str = "") -> ItemFailure:
    return ItemFailure(kind, detail, transient=False)


def backoff_delay(attempts: int, base: float, cap: float) -> float:
    """Exponential backoff with full jitter.

    Jitter matters more than the exponent here: when a host goes down, every
    in-flight item fails at once, and without jitter they would all retry in the
    same instant forever. Randomising across the whole interval spreads them out.
    """
    ceiling = min(cap, base * (2 ** max(0, attempts - 1)))
    return random.uniform(0, ceiling)


def parse_retry_after(value: str | None) -> float | None:
    """Retry-After is either a delay in seconds or an HTTP date."""
    if not value:
        return None
    value = value.strip()
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=UTC)
    return max(0.0, (when - datetime.now(UTC)).total_seconds())
