from collections.abc import Generator

from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.orm import Session, sessionmaker

from database.settings import get_settings


def _build_url() -> URL:
    settings = get_settings()
    return URL.create(
        "mssql+pyodbc",
        username=settings.db_user,
        password=settings.db_password,
        host=settings.db_server,
        port=settings.db_port,
        database=settings.db_name,
        query={
            "driver": settings.db_driver,
            "Encrypt": settings.db_encrypt,
            "TrustServerCertificate": settings.db_trust_server_certificate,
        },
    )


# fast_executemany batches the INSERT of a submitted batch's items into a single
# round trip, which matters when a client posts a few thousand URLs at once.
# The generous login timeout covers an Azure SQL serverless tier resuming from
# auto-pause, which takes longer than the driver's 15s default.
engine = create_engine(
    _build_url(),
    pool_pre_ping=True,
    fast_executemany=True,
    connect_args={"timeout": 60},
    # The workers write from a thread pool, so the connection pool has to be
    # wider than the default 5 or they serialise on checkout.
    pool_size=20,
    max_overflow=10,
    pool_recycle=1800,
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
