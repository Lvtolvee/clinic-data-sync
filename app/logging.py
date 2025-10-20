from __future__ import annotations
import logging
import os
import sys
import time
from pathlib import Path
from functools import wraps
from logging.handlers import RotatingFileHandler

_LOG_FORMAT = "[%(asctime)s] [%(levelname)s] %(message)s"
_DATEFMT = "%Y-%m-%d %H:%M:%S"

class _Formatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        ct = time.localtime(record.created)
        return time.strftime(datefmt or _DATEFMT, ct)

def _ensure_parent(path: str | os.PathLike) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)

def setup_logging(
    level: str = os.getenv("LOG_LEVEL", "INFO"),
    log_file: str | None = os.getenv("LOG_FILE", "logs/app.log"),
    audit_log_file: str | None = os.getenv("AUDIT_LOG_FILE", "logs/audit.log"),
) -> None:
    root = logging.getLogger()
    if getattr(root, "_configured", False):
        return

    root.setLevel(level.upper())
    fmt = _Formatter(_LOG_FORMAT, datefmt=_DATEFMT)

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    root.addHandler(ch)

    if log_file:
        _ensure_parent(log_file)
        fh = RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=10, encoding="utf-8")
        fh.setFormatter(fmt)
        root.addHandler(fh)

    if audit_log_file:
        _ensure_parent(audit_log_file)
        ah = RotatingFileHandler(audit_log_file, maxBytes=10 * 1024 * 1024, backupCount=10, encoding="utf-8")
        ah.setFormatter(fmt)
        audit = logging.getLogger("audit")
        audit.setLevel(level.upper())
        audit.addHandler(ah)
        audit.addHandler(ch)

    root._configured = True

def get_logger(name: str = "app") -> logging.Logger:
    return logging.getLogger(name)

def audit(message: str, **fields) -> None:
    if fields:
        logging.getLogger("audit").info(f"{message} | {fields}")
    else:
        logging.getLogger("audit").info(message)

def log_call(level: int = logging.INFO, include_args: bool = True, redact: tuple[str, ...] = ("password", "token", "secret")):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger = logging.getLogger(func.__module__)
            arg_str = ""
            if include_args:
                items = []
                from inspect import signature
                bound = signature(func).bind_partial(*args, **kwargs)
                bound.apply_defaults()
                for k, v in bound.arguments.items():
                    if any(x in k.lower() for x in redact):
                        v = "***"
                    items.append(f"{k}={v!r}")
                arg_str = ", ".join(items)

            logger.log(level, f"START {func.__name__}({arg_str})" if include_args else f"START {func.__name__}")
            t0 = time.perf_counter()
            try:
                result = func(*args, **kwargs)
                dt = (time.perf_counter() - t0) * 1000
                logger.log(level, f"END   {func.__name__} in {dt:.1f} ms")
                return result
            except Exception as e:
                dt = (time.perf_counter() - t0) * 1000
                logger.exception(f"ERROR {func.__name__} after {dt:.1f} ms: {e}")
                raise
        return wrapper
    return decorator