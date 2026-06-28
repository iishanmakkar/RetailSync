"""RetailSync configuration."""

from datetime import datetime, timezone
from pathlib import Path
import os


def _get_int_env(name: str, default: int) -> int:
	value = os.getenv(name)
	return int(value) if value is not None and value != "" else default


def _get_str_env(name: str, default: str) -> str:
	value = os.getenv(name)
	return value if value is not None and value != "" else default

# =============================================================================
# PROJECT PATHS
# =============================================================================
BASE_DIR = Path(__file__).resolve().parent.parent
DATASET_DIR = BASE_DIR / "datasets"
BRONZE_DIR = DATASET_DIR / "bronze"
REPORTS_DIR = BASE_DIR / "reports"
METADATA_DIR = BASE_DIR / "metadata"
LOGS_DIR = BASE_DIR / "logs"

# =============================================================================
# RANDOMNESS
# =============================================================================
RANDOM_SEED = _get_int_env("RANDOM_SEED", 42)

# =============================================================================
# OUTPUT FORMAT
# =============================================================================
FILE_FORMAT = _get_str_env("FILE_FORMAT", "parquet")  # parquet or csv
COMPRESSION = _get_str_env("COMPRESSION", "snappy")

# =============================================================================
# RECORD COUNTS
# =============================================================================
CUSTOMERS = _get_int_env("CUSTOMERS", 10_000)
PRODUCTS = _get_int_env("PRODUCTS", 5_000)
ORDERS = _get_int_env("ORDERS", 100_000)
PAYMENTS = _get_int_env("PAYMENTS", 100_000)
INVENTORY = _get_int_env("INVENTORY", 20_000)
DELIVERY = _get_int_env("DELIVERY", 50_000)
SUPPORT = _get_int_env("SUPPORT", 20_000)
MARKETING = _get_int_env("MARKETING", 30_000)

# =============================================================================
# DATA QUALITY
# =============================================================================
NULL_PERCENTAGE = float(_get_str_env("NULL_PERCENTAGE", "0.05"))
DUPLICATE_PERCENTAGE = float(_get_str_env("DUPLICATE_PERCENTAGE", "0.03"))
INVALID_PERCENTAGE = float(_get_str_env("INVALID_PERCENTAGE", "0.02"))

# =============================================================================
# DATES
# =============================================================================
_today = datetime.now(timezone.utc)
YEAR = _today.strftime("%Y")
MONTH = _today.strftime("%m")
DAY = _today.strftime("%d")

# =============================================================================
# PARTITIONS
# =============================================================================
PARTITION_PATH = f"year={YEAR}/month={MONTH}/day={DAY}"
