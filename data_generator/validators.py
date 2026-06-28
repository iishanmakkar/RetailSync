"""Reusable validation helpers for generated RetailSync data."""

from __future__ import annotations

import re

EMAIL_PATTERN = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")
PHONE_PATTERN = re.compile(r"^[0-9+().\-\s]{7,}$")


def validate_email(value: str | None) -> bool:
    if value is None:
        return False
    return bool(EMAIL_PATTERN.match(str(value)))


def validate_phone(value: str | None) -> bool:
    if value is None:
        return False
    return bool(PHONE_PATTERN.match(str(value)))


def validate_price(value: object) -> bool:
    try:
        return float(value) >= 0
    except (TypeError, ValueError):
        return False


def validate_stock(value: object) -> bool:
    try:
        return int(value) >= 0
    except (TypeError, ValueError):
        return False