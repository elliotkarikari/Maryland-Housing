"""
Maryland Viability Atlas - Logging Configuration
Structured JSON logging for production
"""

import logging
import sys

from pythonjsonlogger import jsonlogger

from config.settings import get_settings

settings = get_settings()


def setup_logging(name: str = "maryland_atlas") -> logging.Logger:
    """
    Configure structured logging for the application.

    Args:
        name: Logger name

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, settings.LOG_LEVEL))

    # Remove existing handlers
    logger.handlers = []
    # Avoid double logging when root handlers are configured
    logger.propagate = False

    # Console handler with JSON formatting
    handler = logging.StreamHandler(sys.stdout)

    if settings.ENVIRONMENT == "production":
        # JSON formatter for production (better for log aggregation)
        formatter = jsonlogger.JsonFormatter(
            fmt="%(asctime)s %(name)s %(levelname)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%S"
        )
    else:
        # Human-readable format for development
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Optional: File handler for persistence
    if settings.LOG_DIR:
        import os
        from datetime import datetime

        log_file = os.path.join(settings.LOG_DIR, f"{name}_{datetime.now().strftime('%Y%m%d')}.log")

        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    # Configure root so module loggers without handlers also log to file/console
    root_logger = logging.getLogger()
    root_logger.setLevel(logger.level)
    root_logger.handlers = []
    for handler in logger.handlers:
        root_logger.addHandler(handler)

    return logger


def get_logger(module_name: str) -> logging.Logger:
    """
    Get a logger for a specific module.

    Args:
        module_name: Name of the module (typically __name__)

    Returns:
        Logger instance
    """
    return logging.getLogger(module_name)
