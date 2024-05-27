import logging
import re

from pyspark.errors import PySparkException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from sqlglot import Dialect

from databricks.labs.remorph.reconcile.connectors.data_source import DataSource
from databricks.labs.remorph.reconcile.connectors.secrets import SecretsMixin
from databricks.labs.remorph.reconcile.recon_config import JdbcReaderOptions, Schema
from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


def _get_schema_query(catalog: str, schema: str, table: str):
    # TODO: Ensure that the target_catalog in the configuration is not set to "hive_metastore". The source_catalog
    #  can only be set to "hive_metastore" if the source type is "databricks".
    if catalog == "hive_metastore":
        return f"describe table {catalog}.{schema}.{table}"

    query = f"""select lower(column_name) as col_name, full_data_type as data_type from 
                {catalog}.information_schema.columns where lower(table_catalog)='{catalog}' 
                and lower(table_schema)='{schema}' and lower(table_name) ='{table}' order by 
                col_name"""
    return re.sub(r'\s+', ' ', query)


class DatabricksDataSource(DataSource, SecretsMixin):

    def __init__(
        self,
        engine: Dialect,
        spark: SparkSession,
        ws: WorkspaceClient,
        secret_scope: str,
    ):
        self._engine = engine
        self._spark = spark
        self._ws = ws
        self._secret_scope = secret_scope

    def read_data(
        self,
        catalog: str | None,
        schema: str,
        table: str,
        query: str,
        options: JdbcReaderOptions | None,
    ) -> DataFrame:
        table_with_namespace = f"{catalog}.{schema}.{table}"
        table_query = query.replace(":tbl", table_with_namespace)
        try:
            df = self._spark.sql(table_query)
            return df.select([col(column).alias(column.lower()) for column in df.columns])
        except (RuntimeError, PySparkException) as e:
            return self.log_and_throw_exception(e, "data", table_query)

    def get_schema(
        self,
        catalog: str | None,
        schema: str,
        table: str,
    ) -> list[Schema]:
        catalog_str = catalog if catalog else "hive_metastore"
        schema_query = _get_schema_query(catalog_str, schema, table)
        try:
            schema_df = self._spark.sql(schema_query).where("col_name not like '#%'").distinct()
            return [Schema(field.col_name.lower(), field.data_type.lower()) for field in schema_df.collect()]
        except (RuntimeError, PySparkException) as e:
            return self.log_and_throw_exception(e, "schema", schema_query)
