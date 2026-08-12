from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class ApiSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Comma-separated list, or "*". The demo page is served from a different
    # origin (github.io) to the API (azurewebsites.net), so the browser will
    # preflight every request. There is no auth and no cookies involved, so "*"
    # is a reasonable default here — it would not be on a service with either.
    cors_origins: str = "*"

    # The service is public and unauthenticated, so the only thing standing
    # between it and someone queueing a million fetches is this.
    rate_limit_enabled: bool = True
    rate_limit_window_seconds: float = 60.0
    # Submitting work is expensive and asymmetric — one request can enqueue
    # thousands of outbound fetches — so writes are limited far more tightly
    # than reads. Polling for results is cheap and clients are expected to do it.
    rate_limit_writes_per_window: int = 10
    rate_limit_reads_per_window: int = 240

    @property
    def allowed_origins(self) -> list[str]:
        return [origin.strip() for origin in self.cors_origins.split(",") if origin.strip()]


@lru_cache
def get_api_settings() -> ApiSettings:
    return ApiSettings()
