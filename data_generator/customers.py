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
