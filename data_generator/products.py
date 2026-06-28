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
