import logging

from pythonjsonlogger import jsonlogger

import src.utils.logging as log_utils


def test_setup_logging_production_json(monkeypatch, tmp_path):
    monkeypatch.setattr(log_utils.settings, "ENVIRONMENT", "production", raising=False)
    monkeypatch.setattr(log_utils.settings, "LOG_LEVEL", "INFO", raising=False)
    monkeypatch.setattr(log_utils.settings, "LOG_DIR", str(tmp_path), raising=False)

    logger = log_utils.setup_logging("test_logger_prod")

    assert any(isinstance(h.formatter, jsonlogger.JsonFormatter) for h in logger.handlers)
    assert len(logger.handlers) == 2
    assert any(p.name.startswith("test_logger_prod_") for p in tmp_path.iterdir())


def test_setup_logging_dev_formatter(monkeypatch):
    monkeypatch.setattr(log_utils.settings, "ENVIRONMENT", "development", raising=False)
    monkeypatch.setattr(log_utils.settings, "LOG_LEVEL", "INFO", raising=False)
    monkeypatch.setattr(log_utils.settings, "LOG_DIR", "", raising=False)

    logger = log_utils.setup_logging("test_logger_dev")

    assert any(isinstance(h.formatter, logging.Formatter) for h in logger.handlers)


def test_get_logger_returns_named_logger():
    logger = log_utils.get_logger("my.module")
    assert logger.name == "my.module"
