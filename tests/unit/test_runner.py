import asyncio
import os
import signal
import sys
import threading
import time

import pytest

from payment_service.messaging.runner import run_until_stopped


@pytest.mark.skipif(sys.platform == "win32", reason="os.kill terminates the process on Windows")
def test_signal_from_another_thread_stops_idle_worker_promptly():
    async def worker(stop: asyncio.Event) -> None:
        await asyncio.wait_for(stop.wait(), timeout=5)

    threading.Timer(0.2, os.kill, (os.getpid(), signal.SIGINT)).start()
    started = time.monotonic()
    run_until_stopped(worker)
    assert time.monotonic() - started < 2
