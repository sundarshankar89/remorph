from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass

from pyspark.sql import DataFrame
from sqlglot import expressions as exp

from databricks.labs.remorph.reconcile.constants import ThresholdMode

logger = logging.getLogger(__name__)


@dataclass
class JdbcReaderOptions:
    number_partitions: int
    partition_column: str
    lower_bound: str
    upper_bound: str
    fetch_size: int = 100


@dataclass
class ColumnMapping:
    source_name: str
    target_name: str


@dataclass
class Transformation:
    column_name: str
    source: str
    target: str | None = None


@dataclass
class Thresholds:
    column_name: str
    lower_bound: str
    upper_bound: str
    type: str

    def get_mode(self):
        return (
            ThresholdMode.PERCENTAGE.value
            if "%" in self.lower_bound or "%" in self.upper_bound
            else ThresholdMode.ABSOLUTE.value
        )

    def get_type(self):
        if any(self.type in numeric_type.value.lower() for numeric_type in exp.DataType.NUMERIC_TYPES):
            if self.get_mode() == ThresholdMode.ABSOLUTE.value:
                return ThresholdMode.NUMBER_ABSOLUTE.value
            return ThresholdMode.NUMBER_PERCENTAGE.value

        if any(self.type in numeric_type.value.lower() for numeric_type in exp.DataType.TEMPORAL_TYPES):
            return ThresholdMode.DATETIME.value

        error_message = f"Threshold type {self.type} not supported in column {self.column_name}"
        logger.error(error_message)
        raise ValueError(error_message)


@dataclass
class Filters:
    source: str | None = None
    target: str | None = None


@dataclass
class Table:
    source_name: str
    target_name: str
    join_columns: list[str] | None = None
    jdbc_reader_options: JdbcReaderOptions | None = None
    select_columns: list[str] | None = None
    drop_columns: list[str] | None = None
    column_mapping: list[ColumnMapping] | None = None
    transformations: list[Transformation] | None = None
    thresholds: list[Thresholds] | None = None
    filters: Filters | None = None

    @property
    def to_src_col_map(self):
        if self.column_mapping:
            return {c.source_name: c.target_name for c in self.column_mapping}
        return None

    @property
    def to_tgt_col_map(self):
        if self.column_mapping:
            return {c.target_name: c.source_name for c in self.column_mapping}
        return None

    def get_src_to_tgt_col_mapping(self, cols: list[str] | set[str] | str, layer: str) -> set[str] | str:
        if layer == "source":
            if isinstance(cols, str):
                return cols
            return set(cols)
        if isinstance(cols, list | set):
            columns = set()
            for col in cols:
                columns.add(self.to_src_col_map.get(col, col))
            return columns
        return self.to_src_col_map.get(cols, cols)

    def get_tgt_to_src_col_mapping(self, cols: list[str] | set[str] | str, layer: str) -> set[str] | str:
        if layer == "source":
            return cols
        if isinstance(cols, list | set):
            columns = set()
            for col in cols:
                columns.add(self.to_tgt_col_map.get(col, col))
            return columns
        return self.to_tgt_col_map.get(cols, cols)

    def get_select_columns(self, schema: list[Schema], layer: str) -> set[str]:
        if self.select_columns is None:
            return {sch.column_name for sch in schema}
        if self.to_src_col_map:
            return self.get_src_to_tgt_col_mapping(self.select_columns, layer)
        return set(self.select_columns)

    def get_threshold_columns(self, layer: str) -> set[str]:
        if self.thresholds is None:
            return set()
        return {self.get_src_to_tgt_col_mapping(thresh.column_name, layer) for thresh in self.thresholds}

    def get_join_columns(self, layer: str) -> set[str]:
        if self.join_columns is None:
            return set()
        return {self.get_src_to_tgt_col_mapping(col, layer) for col in self.join_columns}

    def get_drop_columns(self, layer: str) -> set[str]:
        if self.drop_columns is None:
            return set()
        return {self.get_src_to_tgt_col_mapping(col, layer) for col in self.drop_columns}

    def get_transformation_dict(self, layer: str) -> dict[str, str] | None:
        if self.transformations:
            if layer == "source":
                return {t.column_name: t.source for t in self.transformations}
            return {self.get_src_to_tgt_col_mapping(t.column_name, layer): t.target for t in self.transformations}
        return None

    def get_partition_column(self, layer: str) -> set[str]:
        if self.jdbc_reader_options and layer == "source":
            return {self.jdbc_reader_options.partition_column}
        return set()

    def get_filter(self, layer: str) -> str | None:
        if self.filters is None:
            return None
        if layer == "source":
            return self.filters.source
        return self.filters.target


@dataclass
class Schema:
    column_name: str
    data_type: str


@dataclass
class ReconcileOutput:
    missing_in_src: DataFrame
    missing_in_tgt: DataFrame
    mismatch: DataFrame | None = None


@dataclass
class DialectHashConfig:
    dialect: str
    algo: list[Callable]


@dataclass
class SchemaMatchResult:
    source_column: str
    source_datatype: str
    databricks_column: str
    databricks_datatype: str
    is_valid: bool = True


@dataclass
class SchemCompareOutput:
    is_valid: bool
    compare_df: DataFrame
