# app/logging.py
from __future__ import annotations
import logging
import logging.handlers
import sys
import time
from functools import wraps
from inspect import signature

_DEFAULT_FORMAT = (
    "%(asctime)sZ | %(levelname)s | %(name)s | %(message)s"
)
_DEFAULT_DATEFMT = "%Y-%m-%dT%H:%M:%S"

class _UTCFormatter(logging.Formatter):
    converter = time.gmtime  # UTC
    def formatTime(self, record, datefmt=None):
        # базовый формат без миллисекунд — короче и стабильнее
        s = super().formatTime(record, datefmt or _DEFAULT_DATEFMT)
        return s

def _make_console_handler(level: int) -> logging.Handler:
    h = logging.StreamHandler(sys.stdout)
    h.setLevel(level)
    h.setFormatter(_UTCFormatter(_DEFAULT_FORMAT))
    return h

def _make_daily_file_handler(path: str, level: int) -> logging.Handler:
    h = logging.handlers.TimedRotatingFileHandler(
        filename=path,
        when="midnight",
        interval=1,
        backupCount=14,
        encoding="utf-8",
        utc=True,
    )
    h.setLevel(level)
    h.setFormatter(_UTCFormatter(_DEFAULT_FORMAT))
    return h

def _make_audit_file_handler(path: str) -> logging.Handler:
    h = logging.handlers.TimedRotatingFileHandler(
        filename=path,
        when="midnight",
        interval=1,
        backupCount=30,
        encoding="utf-8",
        utc=True,
    )
    h.setLevel(logging.INFO)
    h.setFormatter(_UTCFormatter(_DEFAULT_FORMAT))
    return h

def setup_logging(
    level: str | int = "INFO",
    log_file: str | None = None,
    audit_log_file: str | None = None,
) -> None:
    """
    Инициализация логирования. Вызывать один раз при старте процесса.
    Повторный вызов безопасен — хендлеры не дублируются.
    """
    # уровень
    if isinstance(level, str):
        lvl = logging.getLevelName(level.upper())
        if not isinstance(lvl, int):  # fallback
            lvl = logging.INFO
    else:
        lvl = int(level)

    root = logging.getLogger()
    root.setLevel(lvl)

    # чтобы не плодить хендлеры при повторной инициализации
    _already = getattr(root, "_app_logging_initialized", False)
    if _already:
        return

    # консоль
    root.addHandler(_make_console_handler(lvl))

    # файл (общий)
    if log_file:
        root.addHandler(_make_daily_file_handler(log_file, lvl))

    # audit-логгер (отдельный файл, только INFO+)
    if audit_log_file:
        audit_logger = logging.getLogger("audit")
        audit_logger.setLevel(logging.INFO)
        audit_logger.propagate = False
        audit_logger.addHandler(_make_audit_file_handler(audit_log_file))

    # шумные библиотеки приглушим
    for noisy, lv in {
        "urllib3": logging.WARNING,
        "fdb": logging.WARNING,
        "selenium": logging.WARNING,
        "asyncio": logging.WARNING,
    }.items():
        logging.getLogger(noisy).setLevel(lv)

    # перехват неожиданных исключений
    def _excepthook(exc_type, exc, tb):
        logging.getLogger(__name__).exception("Uncaught exception", exc_info=(exc_type, exc, tb))
    sys.excepthook = _excepthook

    root._app_logging_initialized = True  # флажок

def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)

def get_audit_logger() -> logging.Logger:
    return logging.getLogger("audit")

def log_call(level: int = logging.DEBUG):
    """
    Короткий декоратор: логирует START/END и длительность.
    Не логирует аргументы — прод-безопасно.
    """
    def deco(func):
        log = logging.getLogger(func.__module__)
        sig = signature(func)

        @wraps(func)
        def wrapper(*args, **kwargs):
            log.log(level, f"START {func.__name__}()")
            t0 = time.perf_counter()
            try:
                return func(*args, **kwargs)
            finally:
                dt = (time.perf_counter() - t0) * 1000.0
                log.log(level, f"END   {func.__name__} in {dt:.1f} ms")
        return wrapper
    return deco
