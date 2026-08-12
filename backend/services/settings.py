from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class WorkerSettings(BaseSettings):
    """Everything about how hard we push the network, in one place.

    All of it is env-overridable so the pipeline can be tuned (or throttled to a
    single worker for a repeatable test) without touching code.
    """

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    run_worker_in_app: bool = True

    # --- concurrency -------------------------------------------------------
    # Separate limits per stage: fetching is network-bound and cheap, enrichment
    # is rate-limited and expensive. One shared limit would let slow LLM calls
    # starve the fetcher (a bulkhead, in the cloud-patterns sense).
    fetch_concurrency: int = 20
    enrich_concurrency: int = 5
    # Politeness: never open more than this many connections to one host.
    per_host_concurrency: int = 4
    claim_size: int = 20

    # --- queue timing ------------------------------------------------------
    # Must comfortably exceed the worst-case time to process one item, or the
    # reaper will hand a still-running item to a second worker.
    lease_seconds: int = 120
    poll_interval_seconds: float = 1.0
    reaper_interval_seconds: float = 30.0

    # --- retries -----------------------------------------------------------
    max_attempts: int = 5
    backoff_base_seconds: float = 2.0
    backoff_max_seconds: float = 300.0

    # --- fetching ----------------------------------------------------------
    connect_timeout_seconds: float = 5.0
    read_timeout_seconds: float = 10.0
    total_timeout_seconds: float = 20.0
    max_redirects: int = 5
    max_response_bytes: int = 5_000_000
    max_text_chars: int = 20_000
    user_agent: str = "yantra-enricher/0.1 (+https://example.invalid/bot)"
    # Off by default: a fetcher that will follow a URL to 169.254.169.254 is an
    # SSRF hole. Only enable to point the pipeline at a local test server.
    allow_private_hosts: bool = False

    # --- enrichment --------------------------------------------------------
    enricher: str = "mock"  # "mock" | "openai"
    enrich_timeout_seconds: float = 30.0
    mock_latency_seconds: float = 0.1
    # Injected failures are what make the retry path testable without needing a
    # genuinely flaky network.
    mock_failure_rate: float = 0.0
    openai_model: str = "gpt-4o-mini"
    openai_api_key: str | None = None


@lru_cache
def get_worker_settings() -> WorkerSettings:
    return WorkerSettings()
