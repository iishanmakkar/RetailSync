"""Quality report generation for RetailSync synthetic datasets."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from . import config as settings
    from .validators import validate_email
except ImportError:  # pragma: no cover - direct script execution fallback
    import config as settings
    from validators import validate_email


@dataclass(frozen=True)
class DatasetQualitySummary:
    dataset: str
    generated_records: int
    duplicates_injected: int
    duplicates_detected: int
    null_values: int
    nulls: dict[str, int]
    invalid_emails: int
    future_dobs: int
    business_rules: dict[str, int]
    quality_score: float
    file_format: str
    compression: str
    file_size_mb: float
    columns: int
    schema: list[dict[str, str]]
    partition: dict[str, int]
    execution_time_seconds: float
    rows: int
    memory_mb: float
    disk_mb: float
    generated_at: str


def _count_duplicates(dataframe: pd.DataFrame) -> int:
    return int(dataframe.duplicated().sum())


def _count_nulls(dataframe: pd.DataFrame) -> int:
    return int(dataframe.isna().sum().sum())


def _count_null_column(dataframe: pd.DataFrame, column: str) -> int:
    if column not in dataframe.columns:
        return 0
    return int(dataframe[column].isna().sum())


def _count_invalid_emails(dataframe: pd.DataFrame) -> int:
    if "email" not in dataframe.columns:
        return 0
    return int(sum(not validate_email(value) for value in dataframe["email"].dropna().astype(str)))


def _count_future_dobs(dataframe: pd.DataFrame) -> int:
    if "dob" not in dataframe.columns:
        return 0
    dob_series = pd.to_datetime(dataframe["dob"], errors="coerce")
    partition_date = pd.Timestamp(f"{settings.YEAR}-{settings.MONTH}-{settings.DAY}")
    return int((dob_series > partition_date).sum())


def _schema_from_dataframe(dataframe: pd.DataFrame) -> list[dict[str, str]]:
    return [{"column": column, "type": str(dtype)} for column, dtype in dataframe.dtypes.items()]


def _nulls_by_column(dataframe: pd.DataFrame) -> dict[str, int]:
    null_counts = dataframe.isna().sum()
    return {column: int(count) for column, count in null_counts.items() if int(count) > 0}


def _business_rule_counts(dataset_name: str, dataframe: pd.DataFrame) -> dict[str, int]:
    rules: dict[str, int] = {}

    if dataset_name == "customers":
        if "dob" in dataframe.columns:
            partition_date = pd.Timestamp(f"{settings.YEAR}-{settings.MONTH}-{settings.DAY}")
            dob_series = pd.to_datetime(dataframe["dob"], errors="coerce")
            rules["future_dob"] = int((dob_series > partition_date).sum())
        if "email" in dataframe.columns:
            rules["invalid_email"] = _count_invalid_emails(dataframe)

    elif dataset_name == "products":
        if "price" in dataframe.columns:
            rules["negative_price"] = int((pd.to_numeric(dataframe["price"], errors="coerce") < 0).sum())
        if "cost" in dataframe.columns:
            rules["negative_cost"] = int((pd.to_numeric(dataframe["cost"], errors="coerce") < 0).sum())
        if "rating" in dataframe.columns:
            rating = pd.to_numeric(dataframe["rating"], errors="coerce")
            rules["invalid_rating"] = int(((rating < 1) | (rating > 5)).sum())

    elif dataset_name == "orders":
        if "quantity" in dataframe.columns:
            rules["negative_quantity"] = int((pd.to_numeric(dataframe["quantity"], errors="coerce") < 0).sum())
        if "order_status" in dataframe.columns:
            valid_statuses = {"Pending", "Confirmed", "Shipped", "Delivered", "Cancelled", "Returned"}
            rules["invalid_status"] = int((~dataframe["order_status"].isin(valid_statuses)).sum())

    elif dataset_name == "payments":
        if "amount" in dataframe.columns:
            rules["negative_amount"] = int((pd.to_numeric(dataframe["amount"], errors="coerce") < 0).sum())
        if "status" in dataframe.columns:
            valid_statuses = {"Authorized", "Captured", "Settled", "Failed", "Refunded", "Voided"}
            rules["invalid_status"] = int((~dataframe["status"].isin(valid_statuses)).sum())
        if {"payment_date", "settlement_date"}.issubset(dataframe.columns):
            payment_date = pd.to_datetime(dataframe["payment_date"], errors="coerce")
            settlement_date = pd.to_datetime(dataframe["settlement_date"], errors="coerce")
            rules["settlement_before_payment"] = int((settlement_date < payment_date).sum())

    elif dataset_name == "inventory":
        if "stock_quantity" in dataframe.columns:
            rules["negative_stock"] = int((pd.to_numeric(dataframe["stock_quantity"], errors="coerce") < 0).sum())
        if "available_quantity" in dataframe.columns:
            rules["negative_available"] = int((pd.to_numeric(dataframe["available_quantity"], errors="coerce") < 0).sum())
        if {"stock_quantity", "reorder_level", "reorder_flag"}.issubset(dataframe.columns):
            stock = pd.to_numeric(dataframe["stock_quantity"], errors="coerce")
            reorder_level = pd.to_numeric(dataframe["reorder_level"], errors="coerce")
            reorder_flag = dataframe["reorder_flag"].fillna(False).astype(bool)
            expected = stock <= reorder_level
            rules["reorder_flag_mismatch"] = int((expected != reorder_flag).sum())

    elif dataset_name == "delivery":
        if {"dispatch_date", "actual_delivery_date"}.issubset(dataframe.columns):
            dispatch_date = pd.to_datetime(dataframe["dispatch_date"], errors="coerce")
            actual_delivery_date = pd.to_datetime(dataframe["actual_delivery_date"], errors="coerce")
            rules["delivery_date_before_dispatch"] = int((actual_delivery_date < dispatch_date).sum())
        if "delivery_status" in dataframe.columns:
            valid_statuses = {"Label Created", "Picked Up", "In Transit", "Out for Delivery", "Delivered", "Delayed", "Lost"}
            rules["invalid_status"] = int((~dataframe["delivery_status"].isin(valid_statuses)).sum())

    elif dataset_name == "support":
        if "customer_satisfaction_score" in dataframe.columns:
            score = pd.to_numeric(dataframe["customer_satisfaction_score"], errors="coerce")
            rules["invalid_satisfaction_score"] = int(((score < 1) | (score > 5)).sum())
        if "reopen_count" in dataframe.columns:
            rules["negative_reopen_count"] = int((pd.to_numeric(dataframe["reopen_count"], errors="coerce") < 0).sum())
        if "status" in dataframe.columns:
            valid_statuses = {"Open", "Assigned", "Waiting on Customer", "Resolved", "Closed", "Escalated"}
            rules["invalid_status"] = int((~dataframe["status"].isin(valid_statuses)).sum())

    elif dataset_name == "marketing":
        if "clicks" in dataframe.columns:
            rules["negative_clicks"] = int((pd.to_numeric(dataframe["clicks"], errors="coerce") < 0).sum())
        if "conversion_rate" in dataframe.columns:
            rate = pd.to_numeric(dataframe["conversion_rate"], errors="coerce")
            rules["invalid_conversion_rate"] = int(((rate < 0) | (rate > 100)).sum())
        if {"start_date", "end_date"}.issubset(dataframe.columns):
            start_date = pd.to_datetime(dataframe["start_date"], errors="coerce")
            end_date = pd.to_datetime(dataframe["end_date"], errors="coerce")
            rules["end_before_start"] = int((end_date < start_date).sum())

    return {key: int(value) for key, value in rules.items()}


def _quality_score(
    generated_records: int,
    duplicates_detected: int,
    null_values: int,
    invalid_values: int,
    future_dobs: int,
    columns: int,
) -> float:
    if generated_records <= 0:
        return 0.0

    duplicate_penalty = min(25.0, (duplicates_detected / generated_records) * 35.0)
    null_penalty = min(35.0, (null_values / max(generated_records * max(columns, 1), 1)) * 100.0)
    invalid_penalty = min(25.0, (invalid_values / generated_records) * 60.0)
    future_penalty = min(15.0, (future_dobs / generated_records) * 100.0)
    score = 100.0 - duplicate_penalty - null_penalty - invalid_penalty - future_penalty
    return round(max(score, 0.0), 1)


def _file_size_mb(file_path: Path) -> float:
    return round(file_path.stat().st_size / (1024 * 1024), 2)


def _memory_mb(dataframe: pd.DataFrame) -> float:
    return round(float(dataframe.memory_usage(deep=True).sum()) / (1024 * 1024), 2)


def summarize_dataset(
    dataset_name: str,
    dataframe: pd.DataFrame,
    *,
    record_count: int,
    file_path: Path,
    execution_time_seconds: float,
) -> DatasetQualitySummary:
    generated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    duplicates_detected = _count_duplicates(dataframe)
    null_values = _count_nulls(dataframe)
    invalid_emails = _count_invalid_emails(dataframe)
    future_dobs = _count_future_dobs(dataframe)
    business_rules = _business_rule_counts(dataset_name, dataframe)
    invalid_total = sum(business_rules.values())
    columns = int(dataframe.shape[1])
    return DatasetQualitySummary(
        dataset=dataset_name,
        generated_records=int(len(dataframe)),
        duplicates_injected=int(round(record_count * settings.DUPLICATE_PERCENTAGE)),
        duplicates_detected=duplicates_detected,
        null_values=null_values,
        nulls=_nulls_by_column(dataframe),
        invalid_emails=invalid_emails,
        future_dobs=future_dobs,
        business_rules=business_rules,
        quality_score=_quality_score(int(len(dataframe)), duplicates_detected, null_values, invalid_total, future_dobs, columns),
        file_format=settings.FILE_FORMAT,
        compression=settings.COMPRESSION,
        file_size_mb=_file_size_mb(file_path),
        columns=columns,
        schema=_schema_from_dataframe(dataframe),
        partition={"year": int(settings.YEAR), "month": int(settings.MONTH), "day": int(settings.DAY)},
        execution_time_seconds=round(float(execution_time_seconds), 2),
        rows=int(len(dataframe)),
        memory_mb=_memory_mb(dataframe),
        disk_mb=_file_size_mb(file_path),
        generated_at=generated_at,
    )


def write_quality_report(summaries: list[DatasetQualitySummary]) -> Path:
    settings.REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report_path = settings.REPORTS_DIR / "quality_report.json"

    total_records = sum(summary.generated_records for summary in summaries)
    total_duplicates_detected = sum(summary.duplicates_detected for summary in summaries)
    total_duplicates_injected = sum(summary.duplicates_injected for summary in summaries)
    total_nulls = sum(summary.null_values for summary in summaries)
    average_quality = round(sum(summary.quality_score for summary in summaries) / max(len(summaries), 1), 1)
    total_execution = round(sum(summary.execution_time_seconds for summary in summaries), 2)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "datasets": [asdict(summary) for summary in summaries],
        "summary": {
            "datasets": len(summaries),
            "total_records": total_records,
            "duplicates": total_duplicates_detected,
            "duplicates_injected": total_duplicates_injected,
            "null_values": total_nulls,
            "quality_score": average_quality,
            "execution_time_seconds": total_execution,
            "execution_time": f"{round(total_execution, 2)} sec",
        },
    }
    report_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return report_path


def write_metadata(dataset_name: str, summary: DatasetQualitySummary) -> Path:
    settings.METADATA_DIR.mkdir(parents=True, exist_ok=True)
    metadata_path = settings.METADATA_DIR / f"{dataset_name}.json"
    metadata = asdict(summary)
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    return metadata_path