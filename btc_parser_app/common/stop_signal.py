from __future__ import annotations

import logging
import signal
import threading
from typing import Any

logger = logging.getLogger(__name__)


def install_stop_signal() -> threading.Event:
    """SIGTERM/SIGINT set the returned event instead of killing the process
    mid-write - long-running loops check it between units of work and after
    each pass, so a `kill`/Ctrl-C (or stop.sh) always lands on a clean
    checkpoint."""
    stop = threading.Event()

    def _handle(signum: int, _frame: Any) -> None:
        logger.info("Received signal %d - stopping after the current batch/pass...", signum)
        stop.set()

    signal.signal(signal.SIGTERM, _handle)
    signal.signal(signal.SIGINT, _handle)
    return stop
