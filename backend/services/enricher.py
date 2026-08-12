"""The LLM boundary.

The pipeline only ever sees the `Enricher` protocol, so the mock and a real
provider are interchangeable and neither the workers nor the tests need an API
key. This is also the seam where rate limiting or a cache would go.
"""

import asyncio
import hashlib
import json
import random
import re
from dataclasses import dataclass
from typing import Protocol

from services.errors import ErrorKind, permanent, transient
from services.settings import WorkerSettings

SENTIMENTS = ("positive", "neutral", "negative")


@dataclass(frozen=True, slots=True)
class Enrichment:
    summary: str
    sentiment: str


class Enricher(Protocol):
    async def enrich(self, url: str, text: str) -> Enrichment:
        """Return enrichment for a page, or raise ItemFailure.

        Implementations should raise a *transient* failure for anything the
        caller should retry (rate limits, timeouts, 5xx) and a *permanent* one
        for anything it should not (malformed content, refusal).
        """
        ...


class MockEnricher:
    """Deterministic stand-in for a real model.

    Output is derived from a hash of the page text, so the same page always gets
    the same sentiment — which makes assertions in tests possible.
    Latency and failures are injected separately and *non*-deterministically,
    because the point of them is to exercise the retry path.
    """

    def __init__(self, settings: WorkerSettings) -> None:
        self._latency = settings.mock_latency_seconds
        self._failure_rate = settings.mock_failure_rate

    async def enrich(self, url: str, text: str) -> Enrichment:
        if self._latency:
            # Jittered so mock calls don't complete in lockstep and hide
            # concurrency bugs behind uniform timing.
            await asyncio.sleep(random.uniform(0, 2 * self._latency))

        if self._failure_rate and random.random() < self._failure_rate:
            if random.random() < 0.5:
                raise transient(
                    ErrorKind.LLM_RATE_LIMITED, "mock provider rate limited", retry_after=1.0
                )
            raise transient(ErrorKind.LLM_ERROR, "mock provider transient error")

        digest = hashlib.sha256(text.encode("utf-8")).digest()
        return Enrichment(
            summary=_first_sentences(text),
            sentiment=SENTIMENTS[digest[0] % len(SENTIMENTS)],
        )


_SENTENCE_END = re.compile(r"(?<=[.!?])\s+")


def _first_sentences(text: str, limit: int = 320) -> str:
    """A cheap extractive summary: enough to look like output, honest about
    being a stand-in."""
    collapsed = " ".join(text.split())
    if not collapsed:
        return ""
    summary = ""
    for sentence in _SENTENCE_END.split(collapsed):
        if len(summary) + len(sentence) > limit:
            break
        summary = f"{summary} {sentence}".strip()
    return summary or collapsed[:limit]


_PROMPT = (
    "Summarise the web page in two sentences and judge its overall sentiment. "
    f'Respond with JSON: {{"summary": str, "sentiment": one of {list(SENTIMENTS)}}}.'
)


class OpenAIEnricher:
    """Optional real provider. Never imported unless ENRICHER=openai."""

    def __init__(self, settings: WorkerSettings) -> None:
        from openai import AsyncOpenAI  # imported lazily: not a hard dependency

        if not settings.openai_api_key:
            raise RuntimeError("ENRICHER=openai requires OPENAI_API_KEY")
        self._client = AsyncOpenAI(api_key=settings.openai_api_key)
        self._model = settings.openai_model
        self._max_chars = settings.max_text_chars

    async def enrich(self, url: str, text: str) -> Enrichment:
        from openai import APIStatusError, APITimeoutError, RateLimitError

        try:
            response = await self._client.chat.completions.create(
                model=self._model,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": _PROMPT},
                    {"role": "user", "content": f"URL: {url}\n\n{text[: self._max_chars]}"},
                ],
            )
        except RateLimitError as exc:
            raise transient(ErrorKind.LLM_RATE_LIMITED, str(exc)) from exc
        except APITimeoutError as exc:
            raise transient(ErrorKind.TIMEOUT, str(exc)) from exc
        except APIStatusError as exc:
            # 5xx is the provider's problem and worth retrying; 4xx is ours and
            # retrying an identical request will fail identically.
            if exc.status_code >= 500:
                raise transient(ErrorKind.LLM_ERROR, str(exc)) from exc
            raise permanent(ErrorKind.LLM_ERROR, str(exc)) from exc

        payload = json.loads(response.choices[0].message.content or "{}")
        sentiment = str(payload.get("sentiment", "neutral")).lower()
        return Enrichment(
            summary=str(payload.get("summary", ""))[:4000],
            # A model that ignores the enum shouldn't poison the column.
            sentiment=sentiment if sentiment in SENTIMENTS else "neutral",
        )


def build_enricher(settings: WorkerSettings) -> Enricher:
    if settings.enricher == "openai":
        return OpenAIEnricher(settings)
    return MockEnricher(settings)
