"""Delivery dataset generator for RetailSync."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

try:
    from .base_generator import BaseGenerator
    from .config import DELIVERY, DUPLICATE_PERCENTAGE, INVALID_PERCENTAGE, NULL_PERCENTAGE, RANDOM_SEED
    from .logger import logger
    from .utils import customer_id, delivery_id, order_id
except ImportError:  # pragma: no cover - direct script execution fallback
    from base_generator import BaseGenerator
    from config import DELIVERY, DUPLICATE_PERCENTAGE, INVALID_PERCENTAGE, NULL_PERCENTAGE, RANDOM_SEED
    from logger import logger
    from utils import customer_id, delivery_id, order_id


class DeliveryGenerator(BaseGenerator):
    """Generate last-mile delivery records with controlled messiness."""

    dataset_name = "delivery"

    carriers = ["FedEx", "UPS", "USPS", "DHL", "Amazon Logistics", "OnTrac"]
    statuses = ["Label Created", "Picked Up", "In Transit", "Out for Delivery", "Delivered", "Delayed", "Lost"]
    regions = ["Northeast", "Southeast", "Midwest", "Southwest", "West"]

    def __init__(self, seed: int = RANDOM_SEED) -> None:
        self.seed = seed

    def generate(self, record_count: int = DELIVERY) -> pd.DataFrame:
        """Generate the delivery dataframe without saving it."""

        if record_count <= 0:
            raise ValueError("record_count must be greater than zero")

        fake = Faker()
        fake.seed_instance(self.seed)
        rng = np.random.default_rng(self.seed)

        rows = []
        for index in range(1, record_count + 1):
            dispatch_date = pd.Timestamp(
                fake.date_time_between_dates(
                    datetime_start=pd.Timestamp("2024-01-01").to_pydatetime(),
                    datetime_end=pd.Timestamp("2026-06-28 23:59:59").to_pydatetime(),
                )
            )
            transit_days = int(rng.integers(1, 12))
            delay_days = int(rng.integers(0, 5))
            delivery_days = transit_days + delay_days

            rows.append(
                {
                    "delivery_id": delivery_id(index),
                    "order_id": order_id(int(rng.integers(1, max(2, record_count // 10 + 1)))),
                    "customer_id": customer_id(int(rng.integers(1, max(2, record_count // 10 + 1)))),
                    "carrier": rng.choice(self.carriers),
                    "delivery_status": rng.choice(self.statuses, p=[0.05, 0.08, 0.22, 0.2, 0.35, 0.08, 0.02]),
                    "dispatch_date": dispatch_date,
                    "estimated_delivery_date": dispatch_date + pd.Timedelta(days=transit_days),
                    "actual_delivery_date": dispatch_date + pd.Timedelta(days=delivery_days),
                    "delivery_region": rng.choice(self.regions),
                    "shipping_priority": rng.choice(["Standard", "Express", "Same Day"], p=[0.68, 0.26, 0.06]),
                    "tracking_number": fake.bothify(text="TRK##########"),
                    "delivery_fee": round(float(rng.uniform(2.5, 45.0)), 2),
                    "proof_of_delivery": bool(rng.choice([True, False], p=[0.9, 0.1])),
                    "recipient_signature": fake.name() if rng.random() > 0.15 else pd.NA,
                    "delivery_notes": fake.sentence(nb_words=10),
                    "attempt_count": int(rng.integers(1, 4)),
                }
            )

        dataframe = pd.DataFrame(rows)
        dataframe = self._inject_duplicates(dataframe, rng)
        dataframe = self._inject_nulls(dataframe, rng)
        dataframe = self._inject_invalid_dates(dataframe, rng)
        dataframe = self._inject_inconsistent_casing(dataframe, rng)

        logger.info("Generated %s delivery records", len(dataframe))
        return dataframe

    def generate_and_save(self, record_count: int = DELIVERY) -> Path:
        """Generate delivery data and persist it using the shared base class."""

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
        nullable_columns = ["recipient_signature", "delivery_notes", "actual_delivery_date"]

        for column in nullable_columns:
            null_count = int(len(result) * NULL_PERCENTAGE)
            if null_count <= 0:
                continue
            positions = rng.choice(len(result), size=null_count, replace=False)
            result.loc[result.index[positions], column] = pd.NA

        return result

    @staticmethod
    def _inject_invalid_dates(dataframe: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
        result = dataframe.copy()
        invalid_count = int(len(result) * INVALID_PERCENTAGE)
        if invalid_count <= 0:
            return result

        positions = rng.choice(len(result), size=invalid_count, replace=False)
        for position in positions:
            result.at[result.index[position], "actual_delivery_date"] = result.at[result.index[position], "dispatch_date"] - pd.Timedelta(days=int(rng.integers(1, 3)))
            result.at[result.index[position], "delivery_status"] = "Delivered Late"

        return result

    @staticmethod
    def _inject_inconsistent_casing(dataframe: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
        result = dataframe.copy()
        casing_count = max(1, int(len(result) * 0.03))
        positions = rng.choice(len(result), size=casing_count, replace=False)
        transformations = [str.lower, str.upper, str.title]

        for position in positions:
            column = rng.choice(["carrier", "delivery_status", "delivery_region", "shipping_priority"])
            transform = rng.choice(transformations)
            value = result.at[result.index[position], column]
            if pd.notna(value):
                result.at[result.index[position], column] = transform(str(value))

        return result


def generate_delivery(record_count: int = DELIVERY) -> pd.DataFrame:
    """Convenience helper for callers that only need the dataframe."""

    return DeliveryGenerator().generate(record_count=record_count)


if __name__ == "__main__":
    DeliveryGenerator().generate_and_save()
