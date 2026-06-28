"""Shared base class for all dataset generators."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

try:
    from . import config as settings
    from .logger import logger
    from .utils import ensure_directory
except ImportError:  # pragma: no cover - direct script execution fallback
    import config as settings
    from logger import logger
    from utils import ensure_directory


class BaseGenerator:
    """Base class for every dataset generator."""

    dataset_name = ""

    def save(self, dataframe: pd.DataFrame) -> Path:
        """Persist a dataframe to the configured bronze partition path."""

        if not self.dataset_name:
            raise ValueError("dataset_name must be set on the generator subclass")

        output_directory = settings.BRONZE_DIR / self.dataset_name / settings.PARTITION_PATH
        ensure_directory(output_directory)

        if settings.FILE_FORMAT == "parquet":
            file_path = output_directory / f"{self.dataset_name}.parquet"
            dataframe.to_parquet(
                file_path,
                index=False,
                compression=settings.COMPRESSION,
            )
        elif settings.FILE_FORMAT == "csv":
            file_path = output_directory / f"{self.dataset_name}.csv"
            dataframe.to_csv(file_path, index=False)
        else:
            raise ValueError(f"Unsupported FILE_FORMAT: {settings.FILE_FORMAT}")

        logger.info("%s saved successfully -> %s", self.dataset_name, file_path)
        return file_path
