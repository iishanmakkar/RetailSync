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
