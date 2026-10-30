import asyncio
import signal
from collections.abc import Callable, Coroutine
from functools import partial
from typing import Any

from payment_service.config import DatabaseSettings
from payment_service.logging import configure_logging


def run_until_stopped(worker: Callable[[asyncio.Event], Coroutine[Any, Any, None]]) -> None:
    stop = asyncio.Event()
    with asyncio.Runner() as runner:
        loop = runner.get_loop()
        for signum in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(signum, stop.set)
            except NotImplementedError:
                signal.signal(signum, lambda _signum, _frame: loop.call_soon_threadsafe(stop.set))
        runner.run(worker(stop))


def run_worker(
    settings: DatabaseSettings, worker: Callable[..., Coroutine[Any, Any, None]]
) -> None:
    configure_logging(settings.log_level)
    run_until_stopped(partial(worker, settings))
