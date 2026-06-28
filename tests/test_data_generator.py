"""Unit tests for the RetailSync synthetic data generators."""

from __future__ import annotations

import importlib.util
import unittest


DEPENDENCIES_AVAILABLE = all(
    importlib.util.find_spec(module_name) is not None
    for module_name in ("pandas", "faker", "numpy")
)


class GeneratorContractTests(unittest.TestCase):
    """Validate the public generator contract for each dataset."""

    @unittest.skipUnless(DEPENDENCIES_AVAILABLE, "Generator dependencies are not installed in this environment")
    def test_customer_generator_schema(self) -> None:
        from data_generator.customers import CustomersGenerator

        dataframe = CustomersGenerator().generate(record_count=5)
        self.assertEqual(len(dataframe), 5)
        self.assertIn("customer_id", dataframe.columns)
        self.assertIn("email", dataframe.columns)

    @unittest.skipUnless(DEPENDENCIES_AVAILABLE, "Generator dependencies are not installed in this environment")
    def test_product_generator_schema(self) -> None:
        from data_generator.products import ProductsGenerator

        dataframe = ProductsGenerator().generate(record_count=5)
        self.assertEqual(len(dataframe), 5)
        self.assertIn("product_id", dataframe.columns)
        self.assertIn("price", dataframe.columns)

    @unittest.skipUnless(DEPENDENCIES_AVAILABLE, "Generator dependencies are not installed in this environment")
    def test_order_generator_schema(self) -> None:
        from data_generator.orders import OrdersGenerator

        dataframe = OrdersGenerator().generate(record_count=5)
        self.assertEqual(len(dataframe), 5)
        self.assertIn("order_id", dataframe.columns)
        self.assertIn("total_amount", dataframe.columns)

    @unittest.skipUnless(DEPENDENCIES_AVAILABLE, "Generator dependencies are not installed in this environment")
    def test_payment_generator_schema(self) -> None:
        from data_generator.payments import PaymentsGenerator

        dataframe = PaymentsGenerator().generate(record_count=5)
        self.assertEqual(len(dataframe), 5)
        self.assertIn("payment_id", dataframe.columns)
        self.assertIn("amount", dataframe.columns)

    @unittest.skipUnless(DEPENDENCIES_AVAILABLE, "Generator dependencies are not installed in this environment")
    def test_inventory_generator_schema(self) -> None:
        from data_generator.inventory import InventoryGenerator

        dataframe = InventoryGenerator().generate(record_count=5)
        self.assertEqual(len(dataframe), 5)
        self.assertIn("inventory_id", dataframe.columns)
        self.assertIn("warehouse_id", dataframe.columns)

    @unittest.skipUnless(DEPENDENCIES_AVAILABLE, "Generator dependencies are not installed in this environment")
    def test_delivery_generator_schema(self) -> None:
        from data_generator.delivery import DeliveryGenerator

        dataframe = DeliveryGenerator().generate(record_count=5)
        self.assertEqual(len(dataframe), 5)
        self.assertIn("delivery_id", dataframe.columns)
        self.assertIn("tracking_number", dataframe.columns)

    @unittest.skipUnless(DEPENDENCIES_AVAILABLE, "Generator dependencies are not installed in this environment")
    def test_support_generator_schema(self) -> None:
        from data_generator.support import SupportGenerator

        dataframe = SupportGenerator().generate(record_count=5)
        self.assertEqual(len(dataframe), 5)
        self.assertIn("ticket_id", dataframe.columns)
        self.assertIn("ticket_summary", dataframe.columns)

    @unittest.skipUnless(DEPENDENCIES_AVAILABLE, "Generator dependencies are not installed in this environment")
    def test_marketing_generator_schema(self) -> None:
        from data_generator.marketing import MarketingGenerator

        dataframe = MarketingGenerator().generate(record_count=5)
        self.assertEqual(len(dataframe), 5)
        self.assertIn("campaign_id", dataframe.columns)
        self.assertIn("impressions", dataframe.columns)


class IdentifierHelperTests(unittest.TestCase):
    """Validate stable ID formatting helpers used across datasets."""

    @unittest.skipUnless(DEPENDENCIES_AVAILABLE, "Generator dependencies are not installed in this environment")
    def test_identifier_formats(self) -> None:
        from data_generator.utils import campaign_id, customer_id, delivery_id, order_id, payment_id, product_id, ticket_id, warehouse_id

        self.assertEqual(customer_id(1), "CUS00000001")
        self.assertEqual(product_id(1), "PRD00000001")
        self.assertEqual(order_id(1), "ORD00000001")
        self.assertEqual(payment_id(1), "PAY00000001")
        self.assertEqual(warehouse_id(1), "WAR00001")
        self.assertEqual(ticket_id(1), "TKT00000001")
        self.assertEqual(campaign_id(1), "CMP00000001")
        self.assertEqual(delivery_id(1), "DLV00000001")


if __name__ == "__main__":
    unittest.main()
