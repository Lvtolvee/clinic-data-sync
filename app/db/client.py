from __future__ import annotations
from contextlib import contextmanager
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from app.logging import get_logger, log_call
from typing import Iterator
import fdb

log = get_logger(__name__)

@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=0.5, min=1, max=8),
    retry=retry_if_exception_type((fdb.OperationalError, fdb.DatabaseError)),
)
@log_call()
def _connect(dsn: str, user: str, password: str):
    return fdb.connect(dsn=dsn, user=user, password=password, charset="UTF8")

@contextmanager
def get_connection(settings) -> Iterator[fdb.Connection]:
    pwd = settings.DB_PASSWORD.get_secret_value() if settings.DB_PASSWORD else None
    if not pwd:
        raise RuntimeError("DB_PASSWORD должен быть установлен в переменных среды ОС")
    conn = _connect(settings.firebird_dsn, settings.DB_USER, pwd)
    try:
        yield conn
    finally:
        try:
            conn.close()
        except Exception:
            log.warning("Ошибка при закрытиии соединения с БД")