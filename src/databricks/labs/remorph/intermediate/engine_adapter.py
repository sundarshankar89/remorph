import logging
from pathlib import Path

from databricks.labs.remorph.snow.sql_transpiler import SQLTranspiler

logger = logging.getLogger(__name__)


class EngineAdapter:
    def __init__(self, source: str):
        self.source = source

    def select_engine(self, input_type: str):
        if input_type.lower() not in {"sqlglot"}:
            msg = f"Unsupported input type: {input_type}"
            logger.error(msg)
            raise ValueError(msg)
        return SQLTranspiler(self.source, [])

    def parse_sql_content(self, dag, sql_content: str, file_name: str | Path, engine: str):
        # Not added type hints for dag as it is a cyclic import
        parser = self.select_engine(engine)
        for root_table, child in parser.parse_sql_content(sql_content, file_name):
            dag.add_node(child)
            dag.add_edge(root_table, child)
