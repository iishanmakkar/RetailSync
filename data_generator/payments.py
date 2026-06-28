"""Payment dataset generator for RetailSync."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

try:
    from .base_generator import BaseGenerator
    from .config import DUPLICATE_PERCENTAGE, INVALID_PERCENTAGE, NULL_PERCENTAGE, PAYMENTS, RANDOM_SEED
    from .logger import logger
    from .utils import customer_id, order_id, payment_id
except ImportError:  # pragma: no cover - direct script execution fallback
    from base_generator import BaseGenerator
    from config import DUPLICATE_PERCENTAGE, INVALID_PERCENTAGE, NULL_PERCENTAGE, PAYMENTS, RANDOM_SEED
    from logger import logger
    from utils import customer_id, order_id, payment_id


class PaymentsGenerator(BaseGenerator):
    """Generate realistic payment records with controlled messiness."""

    dataset_name = "payments"

    payment_methods = ["Credit Card", "Debit Card", "PayPal", "Apple Pay", "Google Pay", "Bank Transfer"]
    gateways = ["Stripe", "Adyen", "PayPal", "Square", "Worldpay"]
    currencies = ["USD", "EUR", "GBP", "CAD"]
    statuses = ["Authorized", "Captured", "Settled", "Failed", "Refunded", "Voided"]

    def __init__(self, seed: int = RANDOM_SEED) -> None:
        self.seed = seed

    def generate(self, record_count: int = PAYMENTS) -> pd.DataFrame:
        """Generate the payment dataframe without saving it."""

        if record_count <= 0:
            raise ValueError("record_count must be greater than zero")

        fake = Faker()
        fake.seed_instance(self.seed)
        rng = np.random.default_rng(self.seed)

        rows = []
        for index in range(1, record_count + 1):
            amount = round(float(rng.uniform(5.0, 1500.0)), 2)
            fee = round(amount * float(rng.uniform(0.015, 0.045)), 2)
            tax = round(amount * 0.08, 2)
            paid_at = pd.Timestamp(
                fake.date_time_between_dates(
                    datetime_start=pd.Timestamp("2024-01-01").to_pydatetime(),
                    datetime_end=pd.Timestamp("2026-06-28 23:59:59").to_pydatetime(),
                )
            )
            settled_delta = int(rng.integers(0, 4))

            rows.append(
                {
                    "payment_id": payment_id(index),
                    "order_id": order_id(int(rng.integers(1, max(2, record_count // 8 + 1)))),
                    "customer_id": customer_id(int(rng.integers(1, max(2, record_count // 8 + 1)))),
                    "payment_date": paid_at,
                    "settlement_date": paid_at + pd.Timedelta(days=settled_delta),
                    "payment_method": rng.choice(self.payment_methods),
                    "gateway": rng.choice(self.gateways),
                    "currency": rng.choice(self.currencies, p=[0.72, 0.1, 0.08, 0.1]),
                    "amount": amount,
                    "fee": fee,
                    "tax": tax,
                    "status": rng.choice(self.statuses, p=[0.28, 0.34, 0.18, 0.08, 0.08, 0.04]),
                    "transaction_reference": f"TXN-{fake.bothify(text='????????##')}",
                    "authorization_code": f"AUTH-{fake.bothify(text='####??')}",
                    "is_recurring": bool(rng.choice([True, False], p=[0.12, 0.88])),
                    "risk_score": round(float(rng.uniform(0.0, 100.0)), 2),
                }
            )

        dataframe = pd.DataFrame(rows)
        dataframe = self._inject_duplicates(dataframe, rng)
        dataframe = self._inject_nulls(dataframe, rng)
        dataframe = self._inject_invalid_amounts(dataframe, rng)

        logger.info("Generated %s payment records", len(dataframe))
        return dataframe

    def generate_and_save(self, record_count: int = PAYMENTS) -> Path:
        """Generate payment data and persist it using the shared base class."""

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
        nullable_columns = ["gateway", "authorization_code", "settlement_date", "fee"]

        for column in nullable_columns:
            null_count = int(len(result) * NULL_PERCENTAGE)
            if null_count <= 0:
                continue
            positions = rng.choice(len(result), size=null_count, replace=False)
            result.loc[result.index[positions], column] = pd.NA

        return result

    @staticmethod
    def _inject_invalid_amounts(dataframe: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
        result = dataframe.copy()
        invalid_count = int(len(result) * INVALID_PERCENTAGE)
        if invalid_count <= 0:
            return result

        positions = rng.choice(len(result), size=invalid_count, replace=False)
        for position in positions:
            result.at[result.index[position], "amount"] = round(float(rng.uniform(-1000.0, -1.0)), 2)

        status_positions = rng.choice(len(result), size=max(1, invalid_count // 2), replace=False)
        for position in status_positions:
            result.at[result.index[position], "status"] = "Chargeback-Pending"

        return result


def generate_payments(record_count: int = PAYMENTS) -> pd.DataFrame:
    """Convenience helper for callers that only need the dataframe."""

    return PaymentsGenerator().generate(record_count=record_count)


if __name__ == "__main__":
    PaymentsGenerator().generate_and_save()
