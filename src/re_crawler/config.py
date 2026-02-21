from dataclasses import dataclass
import os


@dataclass(frozen=True)
class Settings:
    app_env: str = "dev"
    log_level: str = "INFO"
    request_timeout: int = 10


def load_settings() -> Settings:
    return Settings(
        app_env=os.getenv("APP_ENV", "dev"),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        request_timeout=int(os.getenv("REQUEST_TIMEOUT", "10")),
    )
