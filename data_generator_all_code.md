# RetailSync `data_generator` Source Bundle

## `data_generator/__init__.py`
```python
"""RetailSync Synthetic Data Generator.

This package generates realistic retail datasets with configurable messiness
for building an enterprise AWS data lake.
"""

__version__ = "1.0.0"
__author__ = "Ishan Makkar"
```

## `data_generator/config.py`
```python
"""RetailSync configuration."""

from pathlib import Path

# =============================================================================
# PROJECT PATHS
# =============================================================================
BASE_DIR = Path(__file__).resolve().parent.parent
DATASET_DIR = BASE_DIR / "datasets"
BRONZE_DIR = DATASET_DIR / "bronze"

# =============================================================================
# RANDOMNESS
# =============================================================================
RANDOM_SEED = 42

# =============================================================================
# OUTPUT FORMAT
# =============================================================================
FILE_FORMAT = "parquet"  # parquet or csv
COMPRESSION = "snappy"

# =============================================================================
# RECORD COUNTS
# =============================================================================
CUSTOMERS = 10_000
PRODUCTS = 5_000
ORDERS = 100_000
PAYMENTS = 100_000
INVENTORY = 20_000
DELIVERY = 50_000
SUPPORT = 20_000
MARKETING = 30_000

# =============================================================================
# DATA QUALITY
# =============================================================================
NULL_PERCENTAGE = 0.05
DUPLICATE_PERCENTAGE = 0.03
INVALID_PERCENTAGE = 0.02

# =============================================================================
# DATES
# =============================================================================
YEAR = "2026"
MONTH = "06"
DAY = "28"

# =============================================================================
# PARTITIONS
# =============================================================================
PARTITION_PATH = f"year={YEAR}/month={MONTH}/day={DAY}"
```

## `data_generator/logger.py`
```python
"""Shared logger configuration for RetailSync generators."""

import logging
import sys

LOGGER_NAME = "RetailSync"

logger = logging.getLogger(LOGGER_NAME)
logger.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(formatter)

if not logger.handlers:
    logger.addHandler(stream_handler)
```

## `data_generator/utils.py`
```python
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
```

## `data_generator/base_generator.py`
```python
"""Shared base class for all dataset generators."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

try:
    from .config import BRONZE_DIR, COMPRESSION, FILE_FORMAT, PARTITION_PATH
    from .logger import logger
    from .utils import ensure_directory
except ImportError:  # pragma: no cover - direct script execution fallback
    from config import BRONZE_DIR, COMPRESSION, FILE_FORMAT, PARTITION_PATH
    from logger import logger
    from utils import ensure_directory


class BaseGenerator:
    """Base class for every dataset generator."""

    dataset_name = ""

    def save(self, dataframe: pd.DataFrame) -> Path:
        """Persist a dataframe to the configured bronze partition path."""

        if not self.dataset_name:
            raise ValueError("dataset_name must be set on the generator subclass")

        output_directory = BRONZE_DIR / self.dataset_name / PARTITION_PATH
        ensure_directory(output_directory)

        if FILE_FORMAT == "parquet":
            file_path = output_directory / f"{self.dataset_name}.parquet"
            dataframe.to_parquet(
                file_path,
                index=False,
                compression=COMPRESSION,
            )
        elif FILE_FORMAT == "csv":
            file_path = output_directory / f"{self.dataset_name}.csv"
            dataframe.to_csv(file_path, index=False)
        else:
            raise ValueError(f"Unsupported FILE_FORMAT: {FILE_FORMAT}")

        logger.info("%s saved successfully -> %s", self.dataset_name, file_path)
        return file_path
```

## `data_generator/customers.py`
```python
"""Customer dataset generator for RetailSync."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
from faker import Faker

try:
	from .base_generator import BaseGenerator
	from .config import (
		CUSTOMERS,
		DAY,
		DUPLICATE_PERCENTAGE,
		INVALID_PERCENTAGE,
		MONTH,
		NULL_PERCENTAGE,
		RANDOM_SEED,
		YEAR,
	)
	from .logger import logger
	from .utils import customer_id
except ImportError:  # pragma: no cover - direct script execution fallback
	from base_generator import BaseGenerator
	from config import (
		CUSTOMERS,
		DAY,
		DUPLICATE_PERCENTAGE,
		INVALID_PERCENTAGE,
		MONTH,
		NULL_PERCENTAGE,
		RANDOM_SEED,
		YEAR,
	)
	from logger import logger
	from utils import customer_id


class CustomersGenerator(BaseGenerator):
	"""Generate realistic customer records with controlled messiness."""

	dataset_name = "customers"

	def __init__(self, seed: int = RANDOM_SEED) -> None:
		self.seed = seed

	def generate(self, record_count: int = CUSTOMERS) -> pd.DataFrame:
		"""Generate the customer dataframe without saving it."""

		if record_count <= 0:
			raise ValueError("record_count must be greater than zero")

		fake = Faker()
		fake.seed_instance(self.seed)
		rng = np.random.default_rng(self.seed)
		partition_date = pd.Timestamp(f"{YEAR}-{MONTH}-{DAY}")

		rows = []
		for index in range(1, record_count + 1):
			first_name = fake.first_name()
			last_name = fake.last_name()
			email = f"{first_name}.{last_name}@{fake.free_email_domain()}".replace(" ", "").lower()
			rows.append(
				{
					"customer_id": customer_id(index),
					"first_name": first_name,
					"last_name": last_name,
					"email": email,
					"phone": fake.phone_number(),
					"gender": rng.choice(["Male", "Female", "Other"], p=[0.48, 0.48, 0.04]),
					"dob": pd.Timestamp(fake.date_of_birth(minimum_age=18, maximum_age=80)),
					"address": fake.street_address(),
					"city": fake.city(),
					"state": fake.state(),
					"country": fake.country(),
					"zipcode": fake.postcode(),
					"registration_date": pd.Timestamp(
						fake.date_between_dates(
							date_start=pd.Timestamp("2018-01-01").date(),
							date_end=partition_date.date(),
						)
					),
					"loyalty_points": int(rng.integers(0, 5001)),
					"customer_status": rng.choice(["Active", "Inactive"], p=[0.9, 0.1]),
				}
			)

		dataframe = pd.DataFrame(rows)
		dataframe = self._inject_duplicates(dataframe, rng)
		dataframe = self._inject_nulls(dataframe, rng)
		dataframe = self._inject_invalid_emails(dataframe, rng)
		dataframe = self._inject_future_dobs(dataframe, rng, partition_date)
		dataframe = self._inject_inconsistent_casing(dataframe, rng)
		dataframe["dob"] = pd.to_datetime(dataframe["dob"]).dt.normalize()
		dataframe["registration_date"] = pd.to_datetime(dataframe["registration_date"]).dt.normalize()

		logger.info("Generated %s customer records", len(dataframe))
		return dataframe

	def generate_and_save(self, record_count: int = CUSTOMERS) -> Path:
		"""Generate customer data and persist it using the shared base class."""

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
		nullable_columns = [
			"email",
			"phone",
			"address",
			"city",
			"state",
			"country",
			"zipcode",
		]

		for column in nullable_columns:
			null_count = int(len(result) * NULL_PERCENTAGE)
			if null_count <= 0:
				continue
			positions = rng.choice(len(result), size=null_count, replace=False)
			result.loc[result.index[positions], column] = pd.NA

		return result

	@staticmethod
	def _inject_invalid_emails(dataframe: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
		result = dataframe.copy()
		invalid_count = int(len(result) * INVALID_PERCENTAGE)
		if invalid_count <= 0:
			return result

		positions = rng.choice(len(result), size=invalid_count, replace=False)
		for offset, position in enumerate(positions, start=1):
			result.at[result.index[position], "email"] = f"invalid-email-{offset}"

		return result

	@staticmethod
	def _inject_future_dobs(
		dataframe: pd.DataFrame,
		rng: np.random.Generator,
		partition_date: pd.Timestamp,
	) -> pd.DataFrame:
		result = dataframe.copy()
		future_count = max(1, int(len(result) * 0.01))
		positions = rng.choice(len(result), size=future_count, replace=False)

		for position in positions:
			future_days = int(rng.integers(30, 365))
			result.at[result.index[position], "dob"] = partition_date + pd.Timedelta(days=future_days)

		return result

	@staticmethod
	def _inject_inconsistent_casing(dataframe: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
		result = dataframe.copy()
		target_columns = ["first_name", "last_name", "city", "state", "country", "customer_status"]
		casing_count = max(1, int(len(result) * 0.03))
		positions = rng.choice(len(result), size=casing_count, replace=False)

		transformations = [str.lower, str.upper, str.title]
		for position in positions:
			column = rng.choice(target_columns)
			transform = rng.choice(transformations)
			value = result.at[result.index[position], column]
			if pd.notna(value):
				result.at[result.index[position], column] = transform(str(value))

		return result


def generate_customers(record_count: int = CUSTOMERS) -> pd.DataFrame:
	"""Convenience helper for callers that only need the dataframe."""

	return CustomersGenerator().generate(record_count=record_count)


if __name__ == "__main__":
	CustomersGenerator().generate_and_save()
```

## `data_generator/products.py`
```python
"""Product dataset generator for RetailSync."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

try:
    from .base_generator import BaseGenerator
    from .config import DUPLICATE_PERCENTAGE, INVALID_PERCENTAGE, NULL_PERCENTAGE, PRODUCTS, RANDOM_SEED
    from .logger import logger
    from .utils import product_id, random_string
except ImportError:  # pragma: no cover - direct script execution fallback
    from base_generator import BaseGenerator
    from config import DUPLICATE_PERCENTAGE, INVALID_PERCENTAGE, NULL_PERCENTAGE, PRODUCTS, RANDOM_SEED
    from logger import logger
    from utils import product_id, random_string


class ProductsGenerator(BaseGenerator):
    """Generate realistic product records with controlled messiness."""

    dataset_name = "products"

    categories = [
        "Electronics",
        "Home",
        "Apparel",
        "Beauty",
        "Sports",
        "Grocery",
        "Automotive",
        "Books",
    ]
    brands = [
        "Apex",
        "Northstar",
        "Vertex",
        "Summit",
        "Nova",
        "Orbit",
        "Prime",
        "Element",
        "Atlas",
        "Pulse",
    ]
    conditions = ["New", "Refurbished", "Open Box"]

    def __init__(self, seed: int = RANDOM_SEED) -> None:
        self.seed = seed

    def generate(self, record_count: int = PRODUCTS) -> pd.DataFrame:
        """Generate the product dataframe without saving it."""

        if record_count <= 0:
            raise ValueError("record_count must be greater than zero")

        fake = Faker()
        fake.seed_instance(self.seed)
        rng = np.random.default_rng(self.seed)

        rows = []
        for index in range(1, record_count + 1):
            category = rng.choice(self.categories)
            brand = rng.choice(self.brands)
            base_name = fake.word().replace(" ", "").title()
            category_label = category[:-1] if category.endswith("s") else category
            name = f"{brand} {base_name} {category_label}"
            price = round(float(rng.uniform(5.0, 500.0)), 2)
            cost = round(price * float(rng.uniform(0.45, 0.85)), 2)
            stock_quantity = int(rng.integers(0, 2000))

            rows.append(
                {
                    "product_id": product_id(index),
                    "sku": f"SKU-{random_string(10)}",
                    "product_name": name,
                    "category": category,
                    "brand": brand,
                    "description": fake.sentence(nb_words=12),
                    "price": price,
                    "cost": cost,
                    "currency": "USD",
                    "stock_quantity": stock_quantity,
                    "reorder_level": int(rng.integers(10, 250)),
                    "product_condition": rng.choice(self.conditions, p=[0.8, 0.1, 0.1]),
                    "rating": round(float(rng.uniform(1.0, 5.0)), 1),
                    "active_flag": bool(rng.choice([True, False], p=[0.95, 0.05])),
                }
            )

        dataframe = pd.DataFrame(rows)
        dataframe = self._inject_duplicates(dataframe, rng)
        dataframe = self._inject_nulls(dataframe, rng)
        dataframe = self._inject_invalid_prices(dataframe, rng)
        dataframe = self._inject_inconsistent_casing(dataframe, rng)

        logger.info("Generated %s product records", len(dataframe))
        return dataframe

    def generate_and_save(self, record_count: int = PRODUCTS) -> Path:
        """Generate product data and persist it using the shared base class."""

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
        nullable_columns = ["description", "brand", "cost", "product_condition"]

        for column in nullable_columns:
            null_count = int(len(result) * NULL_PERCENTAGE)
            if null_count <= 0:
                continue
            positions = rng.choice(len(result), size=null_count, replace=False)
            result.loc[result.index[positions], column] = pd.NA

        return result

    @staticmethod
    def _inject_invalid_prices(dataframe: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
        result = dataframe.copy()
        invalid_count = int(len(result) * INVALID_PERCENTAGE)
        if invalid_count <= 0:
            return result

        positions = rng.choice(len(result), size=invalid_count, replace=False)
        for position in positions:
            result.at[result.index[position], "price"] = round(float(rng.uniform(-200.0, -1.0)), 2)

        return result

    @staticmethod
    def _inject_inconsistent_casing(dataframe: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
        result = dataframe.copy()
        casing_count = max(1, int(len(result) * 0.03))
        positions = rng.choice(len(result), size=casing_count, replace=False)
        transformations = [str.lower, str.upper, str.title]

        for position in positions:
            column = rng.choice(["product_name", "category", "brand", "product_condition"])
            transform = rng.choice(transformations)
            value = result.at[result.index[position], column]
            if pd.notna(value):
                result.at[result.index[position], column] = transform(str(value))

        return result


def generate_products(record_count: int = PRODUCTS) -> pd.DataFrame:
    """Convenience helper for callers that only need the dataframe."""

    return ProductsGenerator().generate(record_count=record_count)


if __name__ == "__main__":
    ProductsGenerator().generate_and_save()
```

## `data_generator/orders.py`
```python
"""Order dataset generator for RetailSync."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

try:
    from .base_generator import BaseGenerator
    from .config import DUPLICATE_PERCENTAGE, INVALID_PERCENTAGE, NULL_PERCENTAGE, ORDERS, RANDOM_SEED
    from .logger import logger
    from .utils import customer_id, order_id, product_id
except ImportError:  # pragma: no cover - direct script execution fallback
    from base_generator import BaseGenerator
    from config import DUPLICATE_PERCENTAGE, INVALID_PERCENTAGE, NULL_PERCENTAGE, ORDERS, RANDOM_SEED
    from logger import logger
    from utils import customer_id, order_id, product_id


class OrdersGenerator(BaseGenerator):
    """Generate realistic order records with controlled messiness."""

    dataset_name = "orders"

    order_statuses = ["Pending", "Confirmed", "Shipped", "Delivered", "Cancelled", "Returned"]
    payment_statuses = ["Unpaid", "Paid", "Failed", "Refunded"]
    channels = ["Web", "Mobile", "Store", "Marketplace"]
    priorities = ["Low", "Medium", "High", "Critical"]

    def __init__(self, seed: int = RANDOM_SEED) -> None:
        self.seed = seed

    def generate(self, record_count: int = ORDERS) -> pd.DataFrame:
        """Generate the order dataframe without saving it."""

        if record_count <= 0:
            raise ValueError("record_count must be greater than zero")

        fake = Faker()
        fake.seed_instance(self.seed)
        rng = np.random.default_rng(self.seed)
        customers_size = max(1, record_count // 10)
        products_size = max(1, record_count // 20)

        rows = []
        for index in range(1, record_count + 1):
            quantity = int(rng.integers(1, 8))
            unit_price = round(float(rng.uniform(5.0, 750.0)), 2)
            discount_rate = float(rng.choice([0.0, 0.05, 0.1, 0.15, 0.2], p=[0.4, 0.2, 0.2, 0.1, 0.1]))
            shipping_cost = round(float(rng.uniform(2.5, 25.0)), 2)
            subtotal = quantity * unit_price
            discount_amount = round(subtotal * discount_rate, 2)
            tax_amount = round((subtotal - discount_amount) * 0.08, 2)
            total_amount = round(subtotal - discount_amount + tax_amount + shipping_cost, 2)
            order_ts = pd.Timestamp(
                fake.date_time_between_dates(
                    datetime_start=pd.Timestamp("2024-01-01").to_pydatetime(),
                    datetime_end=pd.Timestamp("2026-06-28 23:59:59").to_pydatetime(),
                )
            )
            ship_delay_days = int(rng.integers(0, 6))
            delivery_delay_days = ship_delay_days + int(rng.integers(1, 8))

            rows.append(
                {
                    "order_id": order_id(index),
                    "customer_id": customer_id(int(rng.integers(1, customers_size + 1))),
                    "product_id": product_id(int(rng.integers(1, products_size + 1))),
                    "order_date": order_ts,
                    "ship_date": order_ts + pd.Timedelta(days=ship_delay_days),
                    "delivery_date": order_ts + pd.Timedelta(days=delivery_delay_days),
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "discount_rate": round(discount_rate, 2),
                    "discount_amount": discount_amount,
                    "tax_amount": tax_amount,
                    "shipping_cost": shipping_cost,
                    "total_amount": total_amount,
                    "order_status": rng.choice(self.order_statuses, p=[0.12, 0.18, 0.22, 0.34, 0.08, 0.06]),
                    "payment_status": rng.choice(self.payment_statuses, p=[0.05, 0.8, 0.1, 0.05]),
                    "channel": rng.choice(self.channels),
                    "priority": rng.choice(self.priorities, p=[0.35, 0.4, 0.2, 0.05]),
                    "region": fake.state_abbr(),
                }
            )

        dataframe = pd.DataFrame(rows)
        dataframe = self._inject_duplicates(dataframe, rng)
        dataframe = self._inject_nulls(dataframe, rng)
        dataframe = self._inject_invalid_values(dataframe, rng)

        logger.info("Generated %s order records", len(dataframe))
        return dataframe

    def generate_and_save(self, record_count: int = ORDERS) -> Path:
        """Generate order data and persist it using the shared base class."""

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
        nullable_columns = ["shipping_cost", "delivery_date", "priority", "region"]

        for column in nullable_columns:
            null_count = int(len(result) * NULL_PERCENTAGE)
            if null_count <= 0:
                continue
            positions = rng.choice(len(result), size=null_count, replace=False)
            result.loc[result.index[positions], column] = pd.NA

        return result

    @staticmethod
    def _inject_invalid_values(dataframe: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
        result = dataframe.copy()
        invalid_count = int(len(result) * INVALID_PERCENTAGE)
        if invalid_count <= 0:
            return result

        positions = rng.choice(len(result), size=invalid_count, replace=False)
        for position in positions:
            result.at[result.index[position], "quantity"] = int(rng.integers(-5, 0))

        status_positions = rng.choice(len(result), size=max(1, invalid_count // 2), replace=False)
        for position in status_positions:
            result.at[result.index[position], "order_status"] = "Processing++"

        return result


def generate_orders(record_count: int = ORDERS) -> pd.DataFrame:
    """Convenience helper for callers that only need the dataframe."""

    return OrdersGenerator().generate(record_count=record_count)


if __name__ == "__main__":
    OrdersGenerator().generate_and_save()
```

## `data_generator/payments.py`
```python
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
```

## `data_generator/inventory.py`
```python
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
```

## `data_generator/delivery.py`
```python
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
```

## `data_generator/support.py`
```python
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
```

## `data_generator/marketing.py`
```python
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
```

## `data_generator/generate_all.py`
```python
"""Generate all RetailSync synthetic datasets."""

from __future__ import annotations

from pathlib import Path
from typing import Dict

try:
	from .config import CUSTOMERS, ORDERS, PRODUCTS
	from .customers import CustomersGenerator
	from .delivery import DeliveryGenerator
	from .inventory import InventoryGenerator
	from .logger import logger
	from .marketing import MarketingGenerator
	from .orders import OrdersGenerator
	from .payments import PaymentsGenerator
	from .products import ProductsGenerator
	from .support import SupportGenerator
except ImportError:  # pragma: no cover - direct script execution fallback
	from config import CUSTOMERS, ORDERS, PRODUCTS
	from customers import CustomersGenerator
	from delivery import DeliveryGenerator
	from inventory import InventoryGenerator
	from logger import logger
	from marketing import MarketingGenerator
	from orders import OrdersGenerator
	from payments import PaymentsGenerator
	from products import ProductsGenerator
	from support import SupportGenerator


def generate_all() -> Dict[str, Path]:
	"""Generate the current core datasets and persist them to bronze storage."""

	logger.info("Starting RetailSync synthetic data generation")
	outputs = {
		"customers": CustomersGenerator().generate_and_save(record_count=CUSTOMERS),
		"products": ProductsGenerator().generate_and_save(record_count=PRODUCTS),
		"orders": OrdersGenerator().generate_and_save(record_count=ORDERS),
		"payments": PaymentsGenerator().generate_and_save(),
		"inventory": InventoryGenerator().generate_and_save(),
		"delivery": DeliveryGenerator().generate_and_save(),
		"support": SupportGenerator().generate_and_save(),
		"marketing": MarketingGenerator().generate_and_save(),
	}
	logger.info("Completed RetailSync synthetic data generation")
	return outputs


def main() -> None:
	"""CLI entrypoint for generating all datasets."""

	outputs = generate_all()
	for dataset_name, file_path in outputs.items():
		logger.info("%s written to %s", dataset_name, file_path)


if __name__ == "__main__":
	main()
```
