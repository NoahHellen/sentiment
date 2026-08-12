"""Schema for the batch enrichment pipeline.

The `items` table doubles as the work queue: each row is both a unit of work
(claimed by a worker via `status` / `next_attempt_at` / `lease_expires_at`) and
the record holding the enrichment result a client later reads back.
"""

import uuid
from datetime import UTC, datetime
from enum import StrEnum

from sqlalchemy import (
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    UnicodeText,
    Uuid,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from database.base import Base


def utcnow() -> datetime:
    return datetime.now(UTC)


class ItemStatus(StrEnum):
    """Lifecycle of a single URL.

    pending -> fetching -> fetched -> enriching -> done
                  |                       |
                  +-------> failed <------+          (terminal)

    `fetching` and `enriching` are transient: a worker holds them under a lease.
    If the worker dies, the lease expires and the reaper returns the row to the
    preceding durable state (`pending` / `fetched`).
    """

    PENDING = "pending"
    FETCHING = "fetching"
    FETCHED = "fetched"
    ENRICHING = "enriching"
    DONE = "done"
    FAILED = "failed"


# Stored as VARCHAR + CHECK constraint rather than a native DB enum, so the
# values are readable in raw SQL (the claim query) and portable across backends.
# create_constraint defaults to False in SQLAlchemy 2.0 — without it the column
# would accept any 16-character string, and the claim query would silently never
# match a typo'd status.
item_status_enum = Enum(
    ItemStatus,
    name="ck_items_status",
    native_enum=False,
    create_constraint=True,
    length=16,
    values_callable=lambda enum: [member.value for member in enum],
)


class Batch(Base):
    """One client submission. Progress is derived from its items, never stored."""

    __tablename__ = "batches"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    total_items: Mapped[int] = mapped_column(Integer, default=0)

    items: Mapped[list["Item"]] = relationship(
        back_populates="batch",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class Item(Base):
    """A single URL: queue entry, pipeline state, and result in one row."""

    __tablename__ = "items"
    __table_args__ = (
        # Intra-batch dedupe, enforced by the database rather than the request handler.
        UniqueConstraint("batch_id", "url_hash", name="uq_items_batch_url"),
        # Serves the claim query: status = ? AND next_attempt_at <= now.
        Index("ix_items_claim", "status", "next_attempt_at"),
        # Serves the reaper: status IN (transient) AND lease_expires_at < now.
        Index("ix_items_lease", "status", "lease_expires_at"),
        # Serves per-batch progress counts.
        Index("ix_items_batch_status", "batch_id", "status"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    batch_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("batches.id", ondelete="CASCADE"), nullable=False
    )

    url: Mapped[str] = mapped_column(String(2048), nullable=False)
    url_hash: Mapped[str] = mapped_column(String(64), nullable=False)

    # --- queue state -------------------------------------------------------
    status: Mapped[ItemStatus] = mapped_column(
        item_status_enum, nullable=False, default=ItemStatus.PENDING
    )
    # Attempts within the *current* stage: reset when the item advances, so a
    # page that took four tries to fetch still gets a full budget for enrichment.
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    # Incremented only when an item is actually rescheduled after a failure, and
    # never reset. This is what the API reports: 0 means "worked first time",
    # which is the question a client is really asking.
    retries: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    # Retry clock. Backoff is "set this to a future time", not "sleep in the worker".
    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow
    )
    # Held while a worker owns the row; expiry is what makes a crash recoverable.
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # --- fetch stage output ------------------------------------------------
    http_status: Mapped[int | None] = mapped_column(Integer, nullable=True)
    content_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    # Persisted between stages so a failed LLM call never forces a re-fetch.
    text: Mapped[str | None] = mapped_column(UnicodeText, nullable=True)

    # --- enrich stage output -----------------------------------------------
    summary: Mapped[str | None] = mapped_column(UnicodeText, nullable=True)
    sentiment: Mapped[str | None] = mapped_column(String(16), nullable=True)

    # --- failure ------------------------------------------------------------
    error_kind: Mapped[str | None] = mapped_column(String(32), nullable=True)
    error_detail: Mapped[str | None] = mapped_column(String(1000), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )

    batch: Mapped[Batch] = relationship(back_populates="items")
