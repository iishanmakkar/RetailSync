"""Shared logger configuration for RetailSync generators."""

import logging
import sys

try:
    from .config import LOGS_DIR
except ImportError:  # pragma: no cover - direct script execution fallback
    from config import LOGS_DIR

LOGGER_NAME = "RetailSync"

logger = logging.getLogger(LOGGER_NAME)
logger.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(formatter)

LOGS_DIR.mkdir(parents=True, exist_ok=True)
file_handler = logging.FileHandler(LOGS_DIR / "generator.log", encoding="utf-8")
file_handler.setFormatter(formatter)

if not logger.handlers:
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
