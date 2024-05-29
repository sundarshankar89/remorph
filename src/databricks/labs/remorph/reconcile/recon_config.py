from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass

from pyspark.sql import DataFrame
from sqlglot import Dialect
from sqlglot import expressions as exp

logger = logging.getLogger(__name__)


@dataclass
class JdbcReaderOptions:
    number_partitions: int
    partition_column: str
    lower_bound: str
    upper_bound: str
    fetch_size: int = 100

    def __post_init__(self):
        self.partition_column = self.partition_column.lower()


@dataclass
class ColumnMapping:
    source_name: str
    target_name: str

    def __post_init__(self):
        self.source_name = self.source_name.lower()
        self.target_name = self.target_name.lower()


@dataclass
class Transformation:
    column_name: str
    source: str | None = None
    target: str | None = None

    def __post_init__(self):
        self.column_name = self.column_name.lower()


@dataclass
class Thresholds:
    column_name: str
    lower_bound: str
    upper_bound: str
    type: str

    def __post_init__(self):
        self.column_name = self.column_name.lower()
        self.type = self.type.lower()

    def get_mode(self):
        return "percentage" if "%" in self.lower_bound or "%" in self.upper_bound else "absolute"

    def get_type(self):
        if any(self.type in numeric_type.value.lower() for numeric_type in exp.DataType.NUMERIC_TYPES):
            if self.get_mode() == "absolute":
                return "number_absolute"
            return "number_percentage"

        if any(self.type in numeric_type.value.lower() for numeric_type in exp.DataType.TEMPORAL_TYPES):
            return "datetime"
        return None


@dataclass
class Filters:
    source: str | None = None
    target: str | None = None


def to_lower_case(input_list: list[str]) -> list[str]:
    return [element.lower() for element in input_list]


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

    def __post_init__(self):
        self.source_name = self.source_name.lower()
        self.target_name = self.target_name.lower()
        self.select_columns = to_lower_case(self.select_columns) if self.select_columns else None
        self.drop_columns = to_lower_case(self.drop_columns) if self.drop_columns else None
        self.join_columns = to_lower_case(self.join_columns) if self.join_columns else None

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

    def get_src_to_tgt_col_mapping_list(self, cols: list[str], layer: str) -> set[str]:
        if layer == "source":
            return set(cols)
        if self.to_src_col_map:
            return {self.to_src_col_map.get(col, col) for col in cols}
        return set(cols)

    def get_layer_src_to_tgt_col_mapping(self, column_name: str, layer: str) -> str:
        if layer == "source":
            return column_name
        if self.to_src_col_map:
            return self.to_src_col_map.get(column_name, column_name)
        return column_name

    def get_tgt_to_src_col_mapping_list(self, cols: list[str] | set[str]) -> set[str]:
        if self.to_tgt_col_map:
            return {self.to_tgt_col_map.get(col, col) for col in cols}
        return set(cols)

    def get_layer_tgt_to_src_col_mapping(self, column_name: str, layer: str) -> str:
        if layer == "source":
            return column_name
        if self.to_tgt_col_map:
            return self.to_tgt_col_map.get(column_name, column_name)
        return column_name

    def get_select_columns(self, schema: list[Schema], layer: str) -> set[str]:
        if self.select_columns is None:
            return {sch.column_name for sch in schema}
        if self.to_src_col_map:
            return self.get_src_to_tgt_col_mapping_list(self.select_columns, layer)
        return set(self.select_columns)

    def get_threshold_columns(self, layer: str) -> set[str]:
        if self.thresholds is None:
            return set()
        return {self.get_layer_src_to_tgt_col_mapping(thresh.column_name, layer) for thresh in self.thresholds}

    def get_join_columns(self, layer: str) -> set[str]:
        if self.join_columns is None:
            return set()
        return {self.get_layer_src_to_tgt_col_mapping(col, layer) for col in self.join_columns}

    def get_drop_columns(self, layer: str) -> set[str]:
        if self.drop_columns is None:
            return set()
        return {self.get_layer_src_to_tgt_col_mapping(col, layer) for col in self.drop_columns}

    def get_transformation_dict(self, layer: str) -> dict[str, str]:
        if self.transformations:
            if layer == "source":
                return {
                    trans.column_name: (trans.source if trans.source else trans.column_name)
                    for trans in self.transformations
                }
            return {
                self.get_layer_src_to_tgt_col_mapping(trans.column_name, layer): (
                    trans.target if trans.target else self.get_layer_src_to_tgt_col_mapping(trans.column_name, layer)
                )
                for trans in self.transformations
            }
        return {}

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
class MismatchOutput:
    mismatch_df: DataFrame | None = None
    mismatch_columns: list[str] | None = None


@dataclass
class ThresholdOutput:
    threshold_df: DataFrame | None = None
    threshold_mismatch_count: int = 0


@dataclass
class DataReconcileOutput:
    mismatch_count: int = 0
    missing_in_src_count: int = 0
    missing_in_tgt_count: int = 0
    mismatch: MismatchOutput = MismatchOutput()
    missing_in_src: DataFrame | None = None
    missing_in_tgt: DataFrame | None = None
    threshold_output: ThresholdOutput = ThresholdOutput()
    exception: str | None = None


@dataclass
class DialectHashConfig:
    dialect: Dialect
    algo: list[Callable]


@dataclass
class SchemaMatchResult:
    source_column: str
    source_datatype: str
    databricks_column: str
    databricks_datatype: str
    is_valid: bool = True


@dataclass
class SchemaReconcileOutput:
    is_valid: bool
    compare_df: DataFrame | None = None
    exception: str | None = None


@dataclass
class ReconcileProcessDuration:
    start_ts: str
    end_ts: str | None


@dataclass
class StatusOutput:
    row: bool | None = None
    column: bool | None = None
    schema: bool | None = None


@dataclass
class ReconcileTableOutput:
    target_table_name: str
    source_table_name: str
    status: StatusOutput = StatusOutput()
    exception_message: str | None = None


@dataclass
class ReconcileOutput:
    recon_id: str
    results: list[ReconcileTableOutput]
