import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.ratelimit import RateLimitMiddleware
from api.routes import router as batches_router
from api.settings import get_api_settings
from database import models  # noqa: F401  (registers tables on Base.metadata)
from database.base import Base
from database.session import engine, get_db
from services.settings import get_worker_settings
from services.worker import Pipeline

log = logging.getLogger("sentiment.api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Idempotent, so the service starts against an empty database without a
    # separate step. `python -m database.init_db` does the same thing explicitly.
    Base.metadata.create_all(bind=engine)

    settings = get_worker_settings()
    pipeline: Pipeline | None = None
    task: asyncio.Task | None = None
    if settings.run_worker_in_app:
        # Convenience for local runs: one process serves the API and drains the
        # queue. In production these are separate deployments, which is the whole
        # point of the queue living in the database rather than in this process.
        pipeline = Pipeline(settings)
        task = asyncio.create_task(pipeline.run(), name="pipeline")
        log.info("worker pipeline running in-process")

    try:
        yield
    finally:
        if pipeline is not None and task is not None:
            pipeline.stop()
            await task


app = FastAPI(title="Sentiment API", lifespan=lifespan)

_api_settings = get_api_settings()

# Order matters: CORS is added last so it sits outermost and still attaches its
# headers to a 429 from the rate limiter. Without that, a throttled browser
# client sees an opaque CORS error instead of the actual reason.
if _api_settings.rate_limit_enabled:
    app.add_middleware(
        RateLimitMiddleware,
        writes_per_window=_api_settings.rate_limit_writes_per_window,
        reads_per_window=_api_settings.rate_limit_reads_per_window,
        window_seconds=_api_settings.rate_limit_window_seconds,
    )

app.add_middleware(
    CORSMiddleware,
    allow_origins=_api_settings.allowed_origins,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type"],
    # No cookies or auth headers are involved, which is what makes a wildcard
    # origin acceptable here.
    allow_credentials=False,
)

app.include_router(batches_router)


@app.get("/health", tags=["health"])
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/health/db", tags=["health"])
def health_db(db: Session = Depends(get_db)) -> dict[str, str]:
    db.execute(text("SELECT 1"))
    return {"status": "ok"}
