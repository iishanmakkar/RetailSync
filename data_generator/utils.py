"""Utility helpers shared across the data generators."""

import random
import string
from pathlib import Path

from faker import Faker

try:
    from .config import RANDOM_SEED
except ImportError:  # pragma: no cover - direct script execution fallback
    from config import RANDOM_SEED

fake = Faker()
random.seed(RANDOM_SEED)
Faker.seed(RANDOM_SEED)


def chance(probability: float) -> bool:
    """Return True with the given probability."""

    return random.random() < probability


def random_string(length: int = 8) -> str:
	"""Return a random uppercase alphanumeric string."""

	return "".join(
		random.choices(
			string.ascii_uppercase + string.digits,
			k=length,
		)
	)


def customer_id(index: int) -> str:
    return f"CUS{index:08d}"


def product_id(index: int) -> str:
    return f"PRD{index:08d}"


def order_id(index: int) -> str:
    return f"ORD{index:08d}"


def payment_id(index: int) -> str:
    return f"PAY{index:08d}"


def warehouse_id(index: int) -> str:
    return f"WAR{index:05d}"


def ticket_id(index: int) -> str:
    return f"TKT{index:08d}"


def campaign_id(index: int) -> str:
    return f"CMP{index:08d}"


def delivery_id(index: int) -> str:
    return f"DLV{index:08d}"


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
