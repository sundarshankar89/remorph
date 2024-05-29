import logging
from dataclasses import dataclass

from sqlglot.dialects.dialect import Dialect, Dialects, DialectType

from databricks.labs.remorph.helpers.morph_status import ParserError
from databricks.labs.remorph.reconcile.recon_config import Table
from databricks.labs.remorph.snow import databricks, experimental, oracle, snowflake

logger = logging.getLogger(__name__)

SQLGLOT_DIALECTS: dict[str, DialectType] = {
    "bigquery": Dialects.BIGQUERY,
    "databricks": databricks.Databricks,
    "experimental": experimental.DatabricksExperimental,
    "drill": Dialects.DRILL,
    "mssql": Dialects.TSQL,
    "netezza": Dialects.POSTGRES,
    "oracle": oracle.Oracle,
    "postgresql": Dialects.POSTGRES,
    "presto": Dialects.PRESTO,
    "redshift": Dialects.REDSHIFT,
    "snowflake": snowflake.Snow,
    "sqlite": Dialects.SQLITE,
    "teradata": Dialects.TERADATA,
    "trino": Dialects.TRINO,
    "vertica": Dialects.POSTGRES,
}


def get_dialect(engine: str) -> Dialect:
    return Dialect.get_or_raise(SQLGLOT_DIALECTS.get(engine))


def get_key_form_dialect(input_dialect: Dialect) -> str:
    return [source_key for source_key, dialect in SQLGLOT_DIALECTS.items() if dialect == input_dialect][0]


@dataclass
class MorphConfig:
    __file__ = "config.yml"
    __version__ = 1

    source: str
    sdk_config: dict[str, str] | None = None
    input_sql: str | None = None
    output_folder: str | None = None
    skip_validation: bool = False
    catalog_name: str = "transpiler_test"
    schema_name: str = "convertor_test"
    mode: str = "current"

    def get_read_dialect(self):
        return get_dialect(self.source)

    def get_write_dialect(self):
        if self.mode == "experimental":
            return get_dialect("experimental")
        return get_dialect("databricks")


@dataclass
class TableRecon:
    __file__ = "recon_config.yml"
    __version__ = 1

    source_schema: str
    target_catalog: str
    target_schema: str
    tables: list[Table]
    source_catalog: str | None = None

    def __post_init__(self):
        self.source_schema = self.source_schema.lower()
        self.target_schema = self.target_schema.lower()
        self.target_catalog = self.target_catalog.lower()
        self.source_catalog = self.source_catalog.lower() if self.source_catalog else self.source_catalog


@dataclass
class DatabaseConfig:
    source_schema: str
    target_catalog: str
    target_schema: str
    source_catalog: str | None = None


@dataclass
class TranspilationResult:
    transpiled_sql: list[str]
    parse_error_list: list[ParserError]


@dataclass
class ValidationResult:
    validated_sql: str
    exception_msg: str | None
