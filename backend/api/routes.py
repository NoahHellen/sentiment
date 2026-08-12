import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from sqlalchemy import func, insert, select
from sqlalchemy.orm import Session

from api.schemas import BatchAccepted, BatchCreate, BatchResults, ItemResult
from api.urls import fingerprint, normalise
from database.models import Batch, Item, ItemStatus
from database.session import get_db

router = APIRouter(prefix="/batches", tags=["batches"])

# Statuses a worker can still move on. Anything else is terminal.
IN_FLIGHT = (
    ItemStatus.PENDING,
    ItemStatus.FETCHING,
    ItemStatus.FETCHED,
    ItemStatus.ENRICHING,
)

# Endpoints are sync `def` on purpose: pyodbc is a blocking driver, so FastAPI
# runs these in its threadpool rather than stalling the event loop the workers
# share.


@router.post("", status_code=status.HTTP_202_ACCEPTED, response_model=BatchAccepted)
def submit_batch(
    payload: BatchCreate, request: Request, db: Session = Depends(get_db)
) -> BatchAccepted:
    """Accept a batch of URLs and return immediately.

    Nothing is fetched here. The response means "this work is durably recorded",
    not "this work is done" — every URL becomes a pending row that a worker will
    claim, so the batch survives a restart of this process.
    """
    deduped: dict[str, str] = {}
    for url in payload.urls:
        normalised = normalise(str(url))
        deduped.setdefault(fingerprint(normalised), normalised)

    batch = Batch(total_items=len(deduped))
    db.add(batch)
    db.flush()  # assign batch.id without committing

    db.execute(
        insert(Item),
        [
            {"batch_id": batch.id, "url": url, "url_hash": url_hash}
            for url_hash, url in deduped.items()
        ],
    )
    # One transaction: either the batch and all of its items exist, or neither
    # does. There is no window in which a client holds an id for work that was
    # never enqueued.
    db.commit()

    return BatchAccepted(
        batch_id=batch.id,
        total_items=len(deduped),
        duplicates_skipped=len(payload.urls) - len(deduped),
        results_url=str(request.url_for("get_batch_results", batch_id=batch.id)),
    )


@router.get("/{batch_id}", response_model=BatchResults, name="get_batch_results")
def get_batch_results(
    batch_id: uuid.UUID,
    db: Session = Depends(get_db),
    item_status: ItemStatus | None = Query(
        default=None, alias="status", description="only return items in this status"
    ),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> BatchResults:
    """Progress and results for a batch.

    Safe to poll: the counts are a single grouped aggregate over an index, and
    the items page is bounded, so this stays cheap for a batch of any size.
    """
    batch = db.get(Batch, batch_id)
    if batch is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="batch not found")

    tally = db.execute(
        select(Item.status, func.count())
        .where(Item.batch_id == batch_id)
        .group_by(Item.status)
    ).all()
    counts = {member: 0 for member in ItemStatus}
    counts.update({ItemStatus(row_status): total for row_status, total in tally})

    query = select(Item).where(Item.batch_id == batch_id)
    if item_status is not None:
        query = query.where(Item.status == item_status)
    items = db.scalars(query.order_by(Item.id).limit(limit).offset(offset)).all()

    return BatchResults(
        batch_id=batch.id,
        created_at=batch.created_at,
        total_items=batch.total_items,
        counts=counts,
        complete=not any(counts[state] for state in IN_FLIGHT),
        items=[ItemResult.model_validate(item) for item in items],
    )
