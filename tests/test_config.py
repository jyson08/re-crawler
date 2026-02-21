from re_crawler.config import load_settings


def test_load_settings_defaults(monkeypatch):
    monkeypatch.delenv("APP_ENV", raising=False)
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    monkeypatch.delenv("REQUEST_TIMEOUT", raising=False)

    settings = load_settings()

    assert settings.app_env == "dev"
    assert settings.log_level == "INFO"
    assert settings.request_timeout == 10
