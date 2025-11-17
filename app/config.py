from __future__ import annotations
import os
from typing import Optional
from pathlib import Path
from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

_NON_SECRET_KEYS = {
    "DB_HOST", "DB_PORT", "DB_PATH", "DB_USER",
    "BROWSER", "BITRIX_MAIN_URL", "BITRIX_LOGIN",
    "LOG_LEVEL", "LOG_FILE", "AUDIT_LOG_FILE",
}

def load_non_secret_env(dotenv_path: str = ".env") -> None:
    try:
        from dotenv import dotenv_values
    except Exception:
        return

    values = dotenv_values(dotenv_path)
    for k, v in values.items():
        if v is None:
            continue
        if k not in os.environ:
            os.environ[k] = v


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="", extra="ignore")

    DB_HOST: str
    DB_PORT: int = 3050
    DB_PATH: str
    DB_USER: str
    RUN_TIME: str = "22:00"
    BROWSER: str
    BITRIX_MAIN_URL: str
    BITRIX_LOGIN:str
    BITRIX_IMPORT_CONTACT_URL:str
    BITRIX_IMPORT_LEAD_URL:str
    BITRIX_IMPORT_DISK_URL:str
    BITRIX_MODE: str
    BITRIX_CONTACT_ADD_URL: str
    BITRIX_CONTACT_UPDATE_URL: str
    BITRIX_CONTACT_GET_URL: str
    BITRIX_LEAD_ADD_URL: str
    BITRIX_LEAD_UPDATE_URL: str
    BITRIX_LEAD_GET_URL: str
    BITRIX_LEAD_CONTACT_ADD_URL: str
    LOG_LEVEL: str = "INFO"
    LOG_FILE: str = "logs/app.log"
    AUDIT_LOG_FILE: str = "logs/audit.log"

    DB_PASSWORD: Optional[SecretStr] = Field(default=None)
    BITRIX_PASSWORD: Optional[SecretStr] = Field(default=None)

    @property
    def resolved_db_password(self) -> str:
        secret_path = Path("/run/secrets/db_password")
        if secret_path.exists():
            return secret_path.read_text().strip()
        if self.DB_PASSWORD:
            return self.DB_PASSWORD.get_secret_value()
        return os.getenv("DB_PASSWORD", "")

    @property
    def resolved_bitrix_password(self) -> str:
        secret_path = Path("/run/secrets/bitrix_password")
        if secret_path.exists():
            return secret_path.read_text().strip()
        if self.BITRIX_PASSWORD:
            return self.BITRIX_PASSWORD.get_secret_value()
        return os.getenv("BITRIX_PASSWORD", "")

    @property
    def firebird_dsn(self) -> str:
        return f"{self.DB_HOST}/{self.DB_PORT}:{self.DB_PATH}"