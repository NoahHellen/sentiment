import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, HttpUrl

from database.models import ItemStatus

# A single request should not be able to enqueue unbounded work. Rejecting at the
# edge is cheaper than discovering it when the insert times out.
MAX_URLS_PER_BATCH = 5_000


class BatchCreate(BaseModel):
    # HttpUrl restricts the scheme to http/https, which keeps file:// and
    # friends out of the fetcher before they reach a worker.
    urls: list[HttpUrl] = Field(min_length=1, max_length=MAX_URLS_PER_BATCH)


class BatchAccepted(BaseModel):
    batch_id: uuid.UUID
    total_items: int
    duplicates_skipped: int
    results_url: str


class ItemResult(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    url: str
    status: ItemStatus
    # How many times this item had to be retried; 0 means it worked first time.
    # The worker's own per-stage attempt counter is an implementation detail.
    retries: int
    http_status: int | None = None
    summary: str | None = None
    sentiment: str | None = None
    error_kind: str | None = None
    error_detail: str | None = None


class BatchResults(BaseModel):
    batch_id: uuid.UUID
    created_at: datetime
    total_items: int
    # Counts for every status, always present, so a client can render progress
    # without special-casing absent keys.
    counts: dict[ItemStatus, int]
    complete: bool
    items: list[ItemResult]
