"""Support dataset generator for RetailSync."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

try:
    from .base_generator import BaseGenerator
    from .config import DUPLICATE_PERCENTAGE, INVALID_PERCENTAGE, NULL_PERCENTAGE, RANDOM_SEED, SUPPORT
    from .logger import logger
    from .utils import customer_id, ticket_id
except ImportError:  # pragma: no cover - direct script execution fallback
    from base_generator import BaseGenerator
    from config import DUPLICATE_PERCENTAGE, INVALID_PERCENTAGE, NULL_PERCENTAGE, RANDOM_SEED, SUPPORT
    from logger import logger
    from utils import customer_id, ticket_id


class SupportGenerator(BaseGenerator):
    """Generate customer support ticket records with controlled messiness."""

    dataset_name = "support"

    categories = ["Order Issue", "Payment Issue", "Delivery Issue", "Account Issue", "Product Issue", "Refund Request"]
    priorities = ["Low", "Medium", "High", "Critical"]
    channels = ["Email", "Phone", "Chat", "Portal", "Social"]
    statuses = ["Open", "Assigned", "Waiting on Customer", "Resolved", "Closed", "Escalated"]

    def __init__(self, seed: int = RANDOM_SEED) -> None:
        self.seed = seed

    def generate(self, record_count: int = SUPPORT) -> pd.DataFrame:
        """Generate the support dataframe without saving it."""

        if record_count <= 0:
            raise ValueError("record_count must be greater than zero")

        fake = Faker()
        fake.seed_instance(self.seed)
        rng = np.random.default_rng(self.seed)

        rows = []
        for index in range(1, record_count + 1):
            opened_at = pd.Timestamp(
                fake.date_time_between_dates(
                    datetime_start=pd.Timestamp("2024-01-01").to_pydatetime(),
                    datetime_end=pd.Timestamp("2026-06-28 23:59:59").to_pydatetime(),
                )
            )
            resolution_hours = int(rng.integers(1, 168))
            first_response_hours = int(rng.integers(0, max(1, resolution_hours // 2 + 1)))

            rows.append(
                {
                    "ticket_id": ticket_id(index),
                    "customer_id": customer_id(int(rng.integers(1, max(2, record_count // 10 + 1)))),
                    "ticket_category": rng.choice(self.categories),
                    "priority": rng.choice(self.priorities, p=[0.45, 0.32, 0.18, 0.05]),
                    "channel": rng.choice(self.channels),
                    "status": rng.choice(self.statuses, p=[0.16, 0.12, 0.1, 0.28, 0.22, 0.12]),
                    "opened_at": opened_at,
                    "first_response_at": opened_at + pd.Timedelta(hours=first_response_hours),
                    "resolved_at": opened_at + pd.Timedelta(hours=resolution_hours),
                    "assigned_agent": fake.name(),
                    "team_name": rng.choice(["Tier 1", "Tier 2", "Billing", "Returns", "Technical Support"]),
                    "sla_breached": bool(rng.choice([True, False], p=[0.14, 0.86])),
                    "reopen_count": int(rng.integers(0, 4)),
                    "customer_satisfaction_score": round(float(rng.uniform(1.0, 5.0)), 1),
                    "ticket_summary": fake.sentence(nb_words=8),
                }
            )

        dataframe = pd.DataFrame(rows)
        dataframe = self._inject_duplicates(dataframe, rng)
        dataframe = self._inject_nulls(dataframe, rng)
        dataframe = self._inject_invalid_scores(dataframe, rng)
        dataframe = self._inject_inconsistent_casing(dataframe, rng)

        logger.info("Generated %s support records", len(dataframe))
        return dataframe

    def generate_and_save(self, record_count: int = SUPPORT) -> Path:
        """Generate support data and persist it using the shared base class."""

        dataframe = self.generate(record_count=record_count)
        return self.save(dataframe)

    @staticmethod
    def _inject_duplicates(dataframe: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
        duplicate_count = int(len(dataframe) * DUPLICATE_PERCENTAGE)
        if duplicate_count <= 0:
            return dataframe

        duplicate_positions = rng.choice(len(dataframe), size=duplicate_count, replace=False)
        source_positions = rng.choice(len(dataframe), size=duplicate_count, replace=True)

        result = dataframe.copy()
        result.iloc[duplicate_positions] = dataframe.iloc[source_positions].to_numpy()
        return result

    @staticmethod
    def _inject_nulls(dataframe: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
        result = dataframe.copy()
        nullable_columns = ["assigned_agent", "team_name", "resolved_at", "customer_satisfaction_score"]

        for column in nullable_columns:
            null_count = int(len(result) * NULL_PERCENTAGE)
            if null_count <= 0:
                continue
            positions = rng.choice(len(result), size=null_count, replace=False)
            result.loc[result.index[positions], column] = pd.NA

        return result

    @staticmethod
    def _inject_invalid_scores(dataframe: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
        result = dataframe.copy()
        invalid_count = int(len(result) * INVALID_PERCENTAGE)
        if invalid_count <= 0:
            return result

        positions = rng.choice(len(result), size=invalid_count, replace=False)
        for position in positions:
            result.at[result.index[position], "customer_satisfaction_score"] = round(float(rng.uniform(-3.0, 0.0)), 1)

        return result

    @staticmethod
    def _inject_inconsistent_casing(dataframe: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
        result = dataframe.copy()
        casing_count = max(1, int(len(result) * 0.03))
        positions = rng.choice(len(result), size=casing_count, replace=False)
        transformations = [str.lower, str.upper, str.title]

        for position in positions:
            column = rng.choice(["ticket_category", "priority", "channel", "status", "team_name"])
            transform = rng.choice(transformations)
            value = result.at[result.index[position], column]
            if pd.notna(value):
                result.at[result.index[position], column] = transform(str(value))

        return result


def generate_support(record_count: int = SUPPORT) -> pd.DataFrame:
    """Convenience helper for callers that only need the dataframe."""

    return SupportGenerator().generate(record_count=record_count)


if __name__ == "__main__":
    SupportGenerator().generate_and_save()
