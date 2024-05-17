from unittest.mock import create_autospec

import pytest

from databricks.connect import DatabricksSession
from databricks.labs.remorph.config import get_dialect
from databricks.labs.remorph.reconcile.connectors.databricks import DatabricksDataSource
from databricks.labs.remorph.reconcile.connectors.oracle import OracleDataSource
from databricks.labs.remorph.reconcile.connectors.snowflake import SnowflakeDataSource
from databricks.labs.remorph.reconcile.connectors.source_adapter import (
    DataSourceAdapter,
)
from databricks.sdk import WorkspaceClient


def test_create_adapter_for_snowflake_dialect():
    spark = create_autospec(DatabricksSession)
    engine = get_dialect("snowflake")
    ws = create_autospec(WorkspaceClient)
    scope = "scope"

    data_source = DataSourceAdapter().create_adapter(engine, spark, ws, scope)
    snowflake_data_source = SnowflakeDataSource(engine, spark, ws, scope).__class__

    assert isinstance(data_source, snowflake_data_source)


def test_create_adapter_for_oracle_dialect():
    spark = create_autospec(DatabricksSession)
    engine = get_dialect("oracle")
    ws = create_autospec(WorkspaceClient)
    scope = "scope"

    data_source = DataSourceAdapter().create_adapter(engine, spark, ws, scope)
    oracle_data_source = OracleDataSource(engine, spark, ws, scope).__class__

    assert isinstance(data_source, oracle_data_source)


def test_create_adapter_for_databricks_dialect():
    spark = create_autospec(DatabricksSession)
    engine = get_dialect("databricks")
    ws = create_autospec(WorkspaceClient)
    scope = "scope"

    data_source = DataSourceAdapter().create_adapter(engine, spark, ws, scope)
    databricks_data_source = DatabricksDataSource(engine, spark, ws, scope).__class__

    assert isinstance(data_source, databricks_data_source)


def test_raise_exception_for_unknown_dialect():
    spark = create_autospec(DatabricksSession)
    engine = get_dialect("trino")
    ws = create_autospec(WorkspaceClient)
    scope = "scope"

    with pytest.raises(ValueError, match=f"Unsupported source type --> {engine}"):
        DataSourceAdapter().create_adapter(engine, spark, ws, scope)
