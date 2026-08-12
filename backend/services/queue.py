"""Queue operations over the `items` table.

Every function here is synchronous and opens its own short-lived session: they
are called from the async workers via `asyncio.to_thread`, and a Session is not
safe to share across threads.

The claim statement is the heart of the design, and is SQL Server specific:

    UPDATE TOP (n) items WITH (UPDLOCK, READPAST, ROWLOCK) ... OUTPUT inserted.*

`UPDLOCK` takes the write lock up front, `READPAST` skips rows another worker
already holds instead of blocking on them, and `OUTPUT` returns the rows we won
— all in one statement, so two workers can never claim the same item. The
Postgres equivalent is `SELECT ... FOR UPDATE SKIP LOCKED`.
"""

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy import text as sql
from sqlalchemy import update
from sqlalchemy.orm import Session

from database.models import Item, ItemStatus
from database.session import SessionLocal
from services.enricher import Enrichment
from services.errors import ErrorKind


@dataclass(slots=True)
class ClaimedItem:
    id: int
    url: str
    attempts: int
    text: str | None = None


@contextmanager
def session_scope() -> Iterator[Session]:
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


_CLAIM = sql("""
    UPDATE TOP (:limit) items WITH (UPDLOCK, READPAST, ROWLOCK)
       SET status = :to_status,
           attempts = attempts + 1,
           lease_expires_at = :lease,
           updated_at = :now
    OUTPUT inserted.id, inserted.url, inserted.attempts
     WHERE status = :from_status
       AND next_attempt_at <= :now
""")


def claim(
    from_status: ItemStatus,
    to_status: ItemStatus,
    limit: int,
    lease_seconds: int,
    *,
    with_text: bool = False,
) -> list[ClaimedItem]:
    """Atomically take up to `limit` due items and put them under lease."""
    now = datetime.now(UTC)
    with session_scope() as session:
        rows = session.execute(
            _CLAIM,
            {
                "limit": limit,
                "from_status": from_status.value,
                "to_status": to_status.value,
                "lease": now + timedelta(seconds=lease_seconds),
                "now": now,
            },
        ).all()
        claimed = [ClaimedItem(id=r.id, url=r.url, attempts=r.attempts) for r in rows]

        # Fetched text is only needed by the enrich stage, and can be large, so
        # it is loaded separately rather than returned by every OUTPUT clause.
        if with_text and claimed:
            texts = dict(
                session.execute(
                    select(Item.id, Item.text).where(
                        Item.id.in_([item.id for item in claimed])
                    )
                ).all()
            )
            for item in claimed:
                item.text = texts.get(item.id)
    return claimed


def mark_fetched(item_id: int, http_status: int, page_text: str, content_hash: str) -> None:
    """Fetch succeeded. Note `attempts` resets: the counter is per stage, so a
    page that took four tries to fetch still gets a full budget for enrichment."""
    _apply(
        item_id,
        status=ItemStatus.FETCHED,
        attempts=0,
        lease_expires_at=None,
        next_attempt_at=datetime.now(UTC),
        http_status=http_status,
        text=page_text,
        content_hash=content_hash,
        error_kind=None,
        error_detail=None,
    )


def mark_done(item_id: int, enrichment: Enrichment) -> None:
    _apply(
        item_id,
        status=ItemStatus.DONE,
        lease_expires_at=None,
        summary=enrichment.summary,
        sentiment=enrichment.sentiment,
        error_kind=None,
        error_detail=None,
    )


def mark_failed(item_id: int, kind: ErrorKind, detail: str) -> None:
    """Terminal. Either the failure was permanent, or we exhausted the retries."""
    _apply(
        item_id,
        status=ItemStatus.FAILED,
        lease_expires_at=None,
        error_kind=kind.value,
        error_detail=detail[:1000],
    )


def reschedule(
    item_id: int,
    back_to: ItemStatus,
    delay_seconds: float,
    kind: ErrorKind,
    detail: str,
) -> None:
    """Transient failure: return the item to its queue, due in the future.

    Backoff is a timestamp, not a sleep — the worker never blocks on a retry, and
    the delay survives a restart.
    """
    _apply(
        item_id,
        status=back_to,
        lease_expires_at=None,
        next_attempt_at=datetime.now(UTC) + timedelta(seconds=delay_seconds),
        # Counted here rather than at claim time so that a clean first-attempt
        # success reports zero retries.
        retries=Item.retries + 1,
        error_kind=kind.value,
        error_detail=detail[:1000],
    )


def _apply(item_id: int, **values) -> None:
    with session_scope() as session:
        session.execute(update(Item).where(Item.id == item_id).values(**values))


# A worker that dies holds its lease until it expires; these are the states it
# could have died in, and where each returns to.
_RECOVERABLE = {
    ItemStatus.FETCHING: ItemStatus.PENDING,
    ItemStatus.ENRICHING: ItemStatus.FETCHED,
}


def reap_expired_leases() -> dict[str, int]:
    """Return items whose lease has lapsed to their previous durable state.

    This is what makes the pipeline survive `kill -9`: nothing is lost, it just
    becomes claimable again once the lease runs out.
    """
    now = datetime.now(UTC)
    recovered: dict[str, int] = {}
    with session_scope() as session:
        for stuck, back_to in _RECOVERABLE.items():
            result = session.execute(
                update(Item)
                .where(
                    Item.status == stuck,
                    Item.lease_expires_at.is_not(None),
                    Item.lease_expires_at < now,
                )
                .values(status=back_to, lease_expires_at=None, next_attempt_at=now)
            )
            if result.rowcount:
                recovered[stuck.value] = result.rowcount
    return recovered
