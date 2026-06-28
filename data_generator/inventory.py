"""Inventory dataset generator for RetailSync."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

try:
    from .base_generator import BaseGenerator
    from .config import DUPLICATE_PERCENTAGE, INVENTORY, INVALID_PERCENTAGE, NULL_PERCENTAGE, RANDOM_SEED
    from .logger import logger
    from .utils import product_id, warehouse_id
except ImportError:  # pragma: no cover - direct script execution fallback
    from base_generator import BaseGenerator
    from config import DUPLICATE_PERCENTAGE, INVENTORY, INVALID_PERCENTAGE, NULL_PERCENTAGE, RANDOM_SEED
    from logger import logger
    from utils import product_id, warehouse_id


class InventoryGenerator(BaseGenerator):
    """Generate warehouse inventory records with controlled messiness."""

    dataset_name = "inventory"

    locations = [
        "New York",
        "Chicago",
        "Dallas",
        "Los Angeles",
        "Atlanta",
        "Seattle",
        "Miami",
        "Phoenix",
    ]
    suppliers = [
        "Global Supply Co",
        "Northwind Trading",
        "Atlas Distributors",
        "Prime Wholesale",
        "Vertex Logistics",
    ]

    def __init__(self, seed: int = RANDOM_SEED) -> None:
        self.seed = seed

    def generate(self, record_count: int = INVENTORY) -> pd.DataFrame:
        """Generate the inventory dataframe without saving it."""

        if record_count <= 0:
            raise ValueError("record_count must be greater than zero")

        fake = Faker()
        fake.seed_instance(self.seed)
        rng = np.random.default_rng(self.seed)

        rows = []
        for index in range(1, record_count + 1):
            stock_quantity = int(rng.integers(0, 5000))
            reserved_quantity = int(rng.integers(0, max(1, stock_quantity // 4 + 1)))
            available_quantity = max(stock_quantity - reserved_quantity, 0)
            reorder_level = int(rng.integers(25, 500))
            restock_delta = int(rng.integers(1, 31))

            rows.append(
                {
                    "inventory_id": f"INV{index:08d}",
                    "warehouse_id": warehouse_id(int(rng.integers(1, 1000))),
                    "product_id": product_id(int(rng.integers(1, max(2, record_count // 2 + 1)))),
                    "warehouse_location": rng.choice(self.locations),
                    "supplier_name": rng.choice(self.suppliers),
                    "stock_quantity": stock_quantity,
                    "reserved_quantity": reserved_quantity,
                    "available_quantity": available_quantity,
                    "reorder_level": reorder_level,
                    "reorder_flag": stock_quantity <= reorder_level,
                    "last_restock_date": pd.Timestamp(
                        fake.date_between_dates(
                            date_start=pd.Timestamp("2024-01-01").date(),
                            date_end=pd.Timestamp("2026-06-28").date(),
                        )
                    ),
                    "next_restock_date": pd.Timestamp(
                        fake.date_between_dates(
                            date_start=pd.Timestamp("2026-06-28").date(),
                            date_end=pd.Timestamp("2026-06-28") + pd.Timedelta(days=restock_delta),
                        )
                    ),
                    "inventory_status": rng.choice(["In Stock", "Low Stock", "Out of Stock", "Discontinued"], p=[0.6, 0.2, 0.15, 0.05]),
                    "unit_cost": round(float(rng.uniform(2.0, 400.0)), 2),
                    "storage_zone": fake.bothify(text="Z-##"),
                }
            )

        dataframe = pd.DataFrame(rows)
        dataframe = self._inject_duplicates(dataframe, rng)
        dataframe = self._inject_nulls(dataframe, rng)
        dataframe = self._inject_invalid_stock(dataframe, rng)
        dataframe = self._inject_inconsistent_casing(dataframe, rng)

        logger.info("Generated %s inventory records", len(dataframe))
        return dataframe

    def generate_and_save(self, record_count: int = INVENTORY) -> Path:
        """Generate inventory data and persist it using the shared base class."""

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
        nullable_columns = ["supplier_name", "storage_zone", "next_restock_date"]

        for column in nullable_columns:
            null_count = int(len(result) * NULL_PERCENTAGE)
            if null_count <= 0:
                continue
            positions = rng.choice(len(result), size=null_count, replace=False)
            result.loc[result.index[positions], column] = pd.NA

        return result

    @staticmethod
    def _inject_invalid_stock(dataframe: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
        result = dataframe.copy()
        invalid_count = int(len(result) * INVALID_PERCENTAGE)
        if invalid_count <= 0:
            return result

        positions = rng.choice(len(result), size=invalid_count, replace=False)
        for position in positions:
            result.at[result.index[position], "stock_quantity"] = int(rng.integers(-50, 0))
            result.at[result.index[position], "available_quantity"] = int(rng.integers(-25, 0))

        return result

    @staticmethod
    def _inject_inconsistent_casing(dataframe: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
        result = dataframe.copy()
        casing_count = max(1, int(len(result) * 0.03))
        positions = rng.choice(len(result), size=casing_count, replace=False)
        transformations = [str.lower, str.upper, str.title]

        for position in positions:
            column = rng.choice(["warehouse_location", "supplier_name", "inventory_status"])
            transform = rng.choice(transformations)
            value = result.at[result.index[position], column]
            if pd.notna(value):
                result.at[result.index[position], column] = transform(str(value))

        return result


def generate_inventory(record_count: int = INVENTORY) -> pd.DataFrame:
    """Convenience helper for callers that only need the dataframe."""

    return InventoryGenerator().generate(record_count=record_count)


if __name__ == "__main__":
    InventoryGenerator().generate_and_save()
