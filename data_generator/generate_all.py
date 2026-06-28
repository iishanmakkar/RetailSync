"""Generate all RetailSync synthetic datasets."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable
import time

try:
	from . import config as settings
	from .customers import CustomersGenerator
	from .delivery import DeliveryGenerator
	from .inventory import InventoryGenerator
	from .logger import logger
	from .marketing import MarketingGenerator
	from .quality import summarize_dataset, write_metadata, write_quality_report
	from .orders import OrdersGenerator
	from .payments import PaymentsGenerator
	from .products import ProductsGenerator
	from .support import SupportGenerator
except ImportError:  # pragma: no cover - direct script execution fallback
	import config as settings
	from customers import CustomersGenerator
	from delivery import DeliveryGenerator
	from inventory import InventoryGenerator
	from logger import logger
	from marketing import MarketingGenerator
	from quality import summarize_dataset, write_metadata, write_quality_report
	from orders import OrdersGenerator
	from payments import PaymentsGenerator
	from products import ProductsGenerator
	from support import SupportGenerator


@dataclass(frozen=True)
class GenerationOptions:
	customers: int = settings.CUSTOMERS
	products: int = settings.PRODUCTS
	orders: int = settings.ORDERS
	payments: int = settings.PAYMENTS
	inventory: int = settings.INVENTORY
	delivery: int = settings.DELIVERY
	support: int = settings.SUPPORT
	marketing: int = settings.MARKETING
	file_format: str = settings.FILE_FORMAT


def _build_generators(options: GenerationOptions) -> list[tuple[str, object, int]]:
	return [
		("customers", CustomersGenerator(), options.customers),
		("products", ProductsGenerator(), options.products),
		("orders", OrdersGenerator(), options.orders),
		("payments", PaymentsGenerator(), options.payments),
		("inventory", InventoryGenerator(), options.inventory),
		("delivery", DeliveryGenerator(), options.delivery),
		("support", SupportGenerator(), options.support),
		("marketing", MarketingGenerator(), options.marketing),
	]


def generate_all(options: GenerationOptions | None = None) -> Dict[str, Path]:
	"""Generate the current core datasets, persist them, and write quality artifacts."""

	options = options or GenerationOptions()
	settings.FILE_FORMAT = options.file_format

	logger.info("Starting RetailSync synthetic data generation")
	outputs: Dict[str, Path] = {}
	summaries = []
	for dataset_name, generator, record_count in _build_generators(options):
		start_time = time.perf_counter()
		dataframe = generator.generate(record_count=record_count)
		file_path = generator.save(dataframe)
		outputs[dataset_name] = file_path
		execution_time_seconds = time.perf_counter() - start_time
		summary = summarize_dataset(
			dataset_name,
			dataframe,
			record_count=record_count,
			file_path=file_path,
			execution_time_seconds=execution_time_seconds,
		)
		summaries.append(summary)
		write_metadata(dataset_name, summary)
	write_quality_report(summaries)
	logger.info("Completed RetailSync synthetic data generation")
	return outputs


def main() -> None:
	"""CLI entrypoint for generating all datasets."""

	parser = argparse.ArgumentParser(description="Generate RetailSync synthetic datasets")
	parser.add_argument("--customers", type=int, default=settings.CUSTOMERS)
	parser.add_argument("--products", type=int, default=settings.PRODUCTS)
	parser.add_argument("--orders", type=int, default=settings.ORDERS)
	parser.add_argument("--payments", type=int, default=settings.PAYMENTS)
	parser.add_argument("--inventory", type=int, default=settings.INVENTORY)
	parser.add_argument("--delivery", type=int, default=settings.DELIVERY)
	parser.add_argument("--support", type=int, default=settings.SUPPORT)
	parser.add_argument("--marketing", type=int, default=settings.MARKETING)
	parser.add_argument("--format", choices=["parquet", "csv"], default=settings.FILE_FORMAT)
	args = parser.parse_args()

	options = GenerationOptions(
		customers=args.customers,
		products=args.products,
		orders=args.orders,
		payments=args.payments,
		inventory=args.inventory,
		delivery=args.delivery,
		support=args.support,
		marketing=args.marketing,
		file_format=args.format,
	)

	outputs = generate_all(options)
	for dataset_name, file_path in outputs.items():
		logger.info("%s written to %s", dataset_name, file_path)


if __name__ == "__main__":
	main()
