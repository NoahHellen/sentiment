"""Standalone worker process:  python -m services.run_worker

The API can also host the pipeline in-process (RUN_WORKER_IN_APP=true, the
default) which keeps the reviewer's setup to one command. Running it separately
is what you would do in production, so that scaling request handling and scaling
fetch throughput are independent decisions.
"""

import asyncio
import logging
import signal

from services.worker import Pipeline


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s %(message)s",
    )
    pipeline = Pipeline()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        # Graceful shutdown: stop claiming, finish what is in flight. Anything
        # still leased when we exit is recovered by the reaper.
        loop.add_signal_handler(sig, pipeline.stop)

    await pipeline.run()


if __name__ == "__main__":
    asyncio.run(main())
