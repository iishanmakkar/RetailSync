"""Marketing dataset generator for RetailSync."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

try:
    from .base_generator import BaseGenerator
    from .config import DUPLICATE_PERCENTAGE, INVALID_PERCENTAGE, MARKETING, NULL_PERCENTAGE, RANDOM_SEED
    from .logger import logger
    from .utils import campaign_id
except ImportError:  # pragma: no cover - direct script execution fallback
    from base_generator import BaseGenerator
    from config import DUPLICATE_PERCENTAGE, INVALID_PERCENTAGE, MARKETING, NULL_PERCENTAGE, RANDOM_SEED
    from logger import logger
    from utils import campaign_id


class MarketingGenerator(BaseGenerator):
    """Generate marketing campaign records with controlled messiness."""

    dataset_name = "marketing"

    channels = ["Email", "Paid Search", "Organic Search", "Social", "Affiliate", "Display"]
    objectives = ["Awareness", "Acquisition", "Conversion", "Retention"]
    statuses = ["Planned", "Running", "Paused", "Completed", "Cancelled"]

    def __init__(self, seed: int = RANDOM_SEED) -> None:
        self.seed = seed

    def generate(self, record_count: int = MARKETING) -> pd.DataFrame:
        """Generate the marketing dataframe without saving it."""

        if record_count <= 0:
            raise ValueError("record_count must be greater than zero")

        fake = Faker()
        fake.seed_instance(self.seed)
        rng = np.random.default_rng(self.seed)

        rows = []
        for index in range(1, record_count + 1):
            impressions = int(rng.integers(1_000, 500_000))
            clicks = int(rng.integers(10, max(11, impressions // 8 + 1)))
            conversions = int(rng.integers(0, max(1, clicks // 5 + 1)))
            spend = round(float(rng.uniform(100.0, 50_000.0)), 2)
            cpc = round(spend / clicks if clicks else 0.0, 2)
            ctr = round((clicks / impressions) * 100, 2)
            conversion_rate = round((conversions / clicks) * 100, 2) if clicks else 0.0
            campaign_start = pd.Timestamp(
                fake.date_time_between_dates(
                    datetime_start=pd.Timestamp("2024-01-01").to_pydatetime(),
                    datetime_end=pd.Timestamp("2026-06-28 23:59:59").to_pydatetime(),
                )
            )
            duration_days = int(rng.integers(7, 121))

            rows.append(
                {
                    "campaign_id": campaign_id(index),
                    "campaign_name": f"{fake.catch_phrase()} Campaign",
                    "channel": rng.choice(self.channels),
                    "objective": rng.choice(self.objectives),
                    "status": rng.choice(self.statuses, p=[0.18, 0.34, 0.1, 0.3, 0.08]),
                    "start_date": campaign_start,
                    "end_date": campaign_start + pd.Timedelta(days=duration_days),
                    "impressions": impressions,
                    "clicks": clicks,
                    "conversions": conversions,
                    "spend": spend,
                    "cost_per_click": cpc,
                    "click_through_rate": ctr,
                    "conversion_rate": conversion_rate,
                    "audience_size": int(rng.integers(5_000, 2_000_000)),
                    "landing_page": fake.url(),
                    "owner_team": rng.choice(["Growth", "Lifecycle", "Brand", "Performance", "CRM"]),
                }
            )

        dataframe = pd.DataFrame(rows)
        dataframe = self._inject_duplicates(dataframe, rng)
        dataframe = self._inject_nulls(dataframe, rng)
        dataframe = self._inject_invalid_metrics(dataframe, rng)
        dataframe = self._inject_inconsistent_casing(dataframe, rng)

        logger.info("Generated %s marketing records", len(dataframe))
        return dataframe

    def generate_and_save(self, record_count: int = MARKETING) -> Path:
        """Generate marketing data and persist it using the shared base class."""

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
        nullable_columns = ["owner_team", "landing_page", "end_date"]

        for column in nullable_columns:
            null_count = int(len(result) * NULL_PERCENTAGE)
            if null_count <= 0:
                continue
            positions = rng.choice(len(result), size=null_count, replace=False)
            result.loc[result.index[positions], column] = pd.NA

        return result

    @staticmethod
    def _inject_invalid_metrics(dataframe: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
        result = dataframe.copy()
        invalid_count = int(len(result) * INVALID_PERCENTAGE)
        if invalid_count <= 0:
            return result

        positions = rng.choice(len(result), size=invalid_count, replace=False)
        for position in positions:
            result.at[result.index[position], "clicks"] = int(rng.integers(-500, 0))
            result.at[result.index[position], "conversion_rate"] = round(float(rng.uniform(101.0, 250.0)), 2)

        return result

    @staticmethod
    def _inject_inconsistent_casing(dataframe: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
        result = dataframe.copy()
        casing_count = max(1, int(len(result) * 0.03))
        positions = rng.choice(len(result), size=casing_count, replace=False)
        transformations = [str.lower, str.upper, str.title]

        for position in positions:
            column = rng.choice(["campaign_name", "channel", "objective", "status", "owner_team"])
            transform = rng.choice(transformations)
            value = result.at[result.index[position], column]
            if pd.notna(value):
                result.at[result.index[position], column] = transform(str(value))

        return result


def generate_marketing(record_count: int = MARKETING) -> pd.DataFrame:
    """Convenience helper for callers that only need the dataframe."""

    return MarketingGenerator().generate(record_count=record_count)


if __name__ == "__main__":
    MarketingGenerator().generate_and_save()
