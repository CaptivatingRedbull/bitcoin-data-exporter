from __future__ import annotations

import logging
import logging.handlers

from btc_parser_app.config import LoggingConfig

_FORMAT = "%(asctime)s %(levelname)-7s %(name)s: %(message)s"
_DATEFMT = "%Y-%m-%dT%H:%M:%S%z"


def configure_logging(config: LoggingConfig, component: str | None = None) -> None:
    """Console logging is always on. If `component` is given (the CLI
    command name, e.g. "rpc-ingest"), also log to a rotating file at
    config.log_dir/<component>.log - this is what start.sh's
    background/detached services rely on, since their stdout only goes to
    a raw *.out redirect."""
    formatter = logging.Formatter(_FORMAT, datefmt=_DATEFMT)
    handlers: list[logging.Handler] = [logging.StreamHandler()]

    if component is not None:
        config.log_dir.mkdir(parents=True, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            config.log_dir / f"{component}.log",
            maxBytes=20 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        )
        handlers.append(file_handler)

    for handler in handlers:
        handler.setFormatter(formatter)

    logging.basicConfig(
        level=getattr(logging, config.level, logging.INFO),
        handlers=handlers,
        force=True,
    )
