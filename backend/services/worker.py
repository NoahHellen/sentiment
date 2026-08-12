"""The worker pool.

Two stages — fetch and enrich — each running as an independent loop over the
queue table:

    claim a bounded slice  ->  process concurrently  ->  write the outcome

Plus a reaper that returns items whose worker died. Nothing here holds state
that matters: kill the process at any point and the database still describes
exactly what remains to be done.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from database.models import ItemStatus
from services.enricher import Enricher, build_enricher
from services.errors import ErrorKind, ItemFailure, backoff_delay, permanent, transient
from services.fetcher import Fetcher
from services.queue import (
    ClaimedItem,
    claim,
    mark_done,
    mark_failed,
    mark_fetched,
    reap_expired_leases,
    reschedule,
)
from services.settings import WorkerSettings, get_worker_settings

log = logging.getLogger("yantra.worker")


@dataclass(frozen=True, slots=True)
class Stage:
    name: str
    claim_from: ItemStatus
    claim_to: ItemStatus
    concurrency: int
    needs_text: bool
    process: Callable[[ClaimedItem], Awaitable[None]]


class Pipeline:
    def __init__(
        self,
        settings: WorkerSettings | None = None,
        fetcher: Fetcher | None = None,
        enricher: Enricher | None = None,
    ) -> None:
        self._settings = settings or get_worker_settings()
        self._fetcher = fetcher or Fetcher(self._settings)
        self._enricher = enricher or build_enricher(self._settings)
        self._stopping = asyncio.Event()

    # ------------------------------------------------------------------ stages

    def _stages(self) -> list[Stage]:
        return [
            Stage(
                name="fetch",
                claim_from=ItemStatus.PENDING,
                claim_to=ItemStatus.FETCHING,
                concurrency=self._settings.fetch_concurrency,
                needs_text=False,
                process=self._fetch,
            ),
            Stage(
                name="enrich",
                claim_from=ItemStatus.FETCHED,
                claim_to=ItemStatus.ENRICHING,
                concurrency=self._settings.enrich_concurrency,
                needs_text=True,
                process=self._enrich,
            ),
        ]

    async def _fetch(self, item: ClaimedItem) -> None:
        result = await self._fetcher.fetch(item.url)
        await asyncio.to_thread(
            mark_fetched, item.id, result.http_status, result.text, result.content_hash
        )

    async def _enrich(self, item: ClaimedItem) -> None:
        if not item.text:
            raise permanent(ErrorKind.EMPTY_CONTENT, "no stored text to enrich")
        try:
            enrichment = await asyncio.wait_for(
                self._enricher.enrich(item.url, item.text),
                timeout=self._settings.enrich_timeout_seconds,
            )
        except TimeoutError as exc:
            raise transient(ErrorKind.TIMEOUT, "enricher timed out") from exc
        await asyncio.to_thread(mark_done, item.id, enrichment)

    # ------------------------------------------------------------------- loops

    async def run(self) -> None:
        log.info(
            "pipeline starting (fetch=%d enrich=%d per_host=%d enricher=%s)",
            self._settings.fetch_concurrency,
            self._settings.enrich_concurrency,
            self._settings.per_host_concurrency,
            self._settings.enricher,
        )
        loops = [asyncio.create_task(self._run_stage(s), name=s.name) for s in self._stages()]
        loops.append(asyncio.create_task(self._run_reaper(), name="reaper"))
        try:
            await asyncio.gather(*loops)
        finally:
            await self._fetcher.aclose()
            log.info("pipeline stopped")

    def stop(self) -> None:
        """Ask every loop to finish its in-flight work and exit."""
        self._stopping.set()

    async def _run_stage(self, stage: Stage) -> None:
        in_flight: set[asyncio.Task] = set()
        while not self._stopping.is_set():
            # Only ever claim what we have capacity to start. This is the
            # backpressure: an unbounded batch is processed in bounded slices,
            # and no item is taken under lease before we can work on it.
            if len(in_flight) >= stage.concurrency:
                await asyncio.wait(in_flight, return_when=asyncio.FIRST_COMPLETED)
                continue

            capacity = min(stage.concurrency - len(in_flight), self._settings.claim_size)
            try:
                items = await asyncio.to_thread(
                    claim,
                    stage.claim_from,
                    stage.claim_to,
                    capacity,
                    self._settings.lease_seconds,
                    with_text=stage.needs_text,
                )
            except Exception:
                # The database being briefly unavailable must not kill the loop;
                # the items stay leased and the reaper will free them.
                log.exception("%s: claim failed", stage.name)
                await self._pause(self._settings.poll_interval_seconds)
                continue

            if not items:
                await self._pause(self._settings.poll_interval_seconds)
                continue

            log.debug("%s: claimed %d", stage.name, len(items))
            for item in items:
                task = asyncio.create_task(
                    self._process(stage, item), name=f"{stage.name}:{item.id}"
                )
                in_flight.add(task)
                task.add_done_callback(in_flight.discard)

        if in_flight:
            log.info("%s: draining %d in-flight items", stage.name, len(in_flight))
            await asyncio.gather(*in_flight, return_exceptions=True)

    async def _run_reaper(self) -> None:
        while not self._stopping.is_set():
            await self._pause(self._settings.reaper_interval_seconds)
            if self._stopping.is_set():
                break
            try:
                recovered = await asyncio.to_thread(reap_expired_leases)
            except Exception:
                log.exception("reaper failed")
                continue
            if recovered:
                log.warning("reaper recovered items with expired leases: %s", recovered)

    # -------------------------------------------------------------- outcomes

    async def _process(self, stage: Stage, item: ClaimedItem) -> None:
        try:
            await stage.process(item)
        except ItemFailure as failure:
            await self._record_failure(stage, item, failure)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # An unclassified error is treated as transient: we would rather
            # retry something unretryable than permanently drop a good item on
            # a bug in our own code.
            log.exception("%s: unhandled error on item %d", stage.name, item.id)
            await self._record_failure(
                stage, item, ItemFailure(ErrorKind.UNKNOWN, repr(exc), transient=True)
            )

    async def _record_failure(
        self, stage: Stage, item: ClaimedItem, failure: ItemFailure
    ) -> None:
        exhausted = item.attempts >= self._settings.max_attempts
        if failure.transient and not exhausted:
            delay = (
                failure.retry_after
                if failure.retry_after is not None
                else backoff_delay(
                    item.attempts,
                    self._settings.backoff_base_seconds,
                    self._settings.backoff_max_seconds,
                )
            )
            log.info(
                "%s: item %d failed (%s), retry %d/%d in %.1fs",
                stage.name,
                item.id,
                failure.kind,
                item.attempts,
                self._settings.max_attempts,
                delay,
            )
            await asyncio.to_thread(
                reschedule, item.id, stage.claim_from, delay, failure.kind, failure.detail
            )
            return

        reason = "attempts exhausted" if failure.transient else "permanent"
        log.info("%s: item %d failed (%s, %s)", stage.name, item.id, failure.kind, reason)
        await asyncio.to_thread(mark_failed, item.id, failure.kind, failure.detail)

    async def _pause(self, seconds: float) -> None:
        """Sleep, but wake immediately on shutdown."""
        try:
            await asyncio.wait_for(self._stopping.wait(), timeout=seconds)
        except TimeoutError:
            pass
