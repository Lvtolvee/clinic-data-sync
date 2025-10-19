from __future__ import annotations
import os
from typing import Optional
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
    for k, v in dotenv_values(dotenv_path).items():
        if v is None:
            continue
        if k in _NON_SECRET_KEYS and k not in os.environ:
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
    def firebird_dsn(self) -> str:
        return f"{self.DB_HOST}/{self.DB_PORT}:{self.DB_PATH}"