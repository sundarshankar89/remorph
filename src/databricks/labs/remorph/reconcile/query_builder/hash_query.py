import logging

import sqlglot.expressions as exp
from sqlglot import Dialect

from databricks.labs.remorph.reconcile.query_builder.base import QueryBuilder
from databricks.labs.remorph.reconcile.query_builder.expression_generator import (
    build_column,
    concat,
    get_hash_transform,
    lower,
    transform_expression,
)

logger = logging.getLogger(__name__)


def _hash_transform(
    node: exp.Expression,
    source: Dialect,
):
    transform = get_hash_transform(source)
    return transform_expression(node, transform)


_HASH_COLUMN_NAME = "hash_value_recon"


class HashQueryBuilder(QueryBuilder):

    def build_query(self, report_type: str) -> str:
        hash_cols = sorted((self.join_columns | self.select_columns) - self.threshold_columns - self.drop_columns)

        key_cols = hash_cols if report_type == "row" else sorted(self.join_columns | self.partition_column)

        cols_with_alias = [
            build_column(this=col, alias=self.table_conf.get_layer_tgt_to_src_col_mapping(col, self.layer))
            for col in key_cols
        ]

        # in case if we have column mapping, we need to sort the target columns in the order of source columns to get
        # same hash value
        hash_cols_with_alias = [
            {"this": col, "alias": self.table_conf.get_layer_tgt_to_src_col_mapping(col, self.layer)}
            for col in hash_cols
        ]
        sorted_hash_cols_with_alias = sorted(hash_cols_with_alias, key=lambda column: column["alias"])
        hashcols_sorted_as_src_seq = [column["this"] for column in sorted_hash_cols_with_alias]

        key_cols_with_transform = (
            self._apply_user_transformation(cols_with_alias) if self.user_transformations else cols_with_alias
        )
        hash_col_with_transform = [self._generate_hash_algorithm(hashcols_sorted_as_src_seq, _HASH_COLUMN_NAME)]

        res = (
            exp.select(*hash_col_with_transform + key_cols_with_transform)
            .from_(":tbl")
            .where(self.filter)
            .sql(dialect=self.source)
        )

        logger.info(f"Hash Query for {self.layer}: {res}")
        return res

    def _generate_hash_algorithm(
        self,
        cols: list[str],
        column_alias: str,
    ) -> exp.Expression:
        cols_with_alias = [build_column(this=col, alias=None) for col in cols]
        cols_with_transform = self.add_transformations(cols_with_alias, self.source)
        col_exprs = exp.select(*cols_with_transform).iter_expressions()
        concat_expr = concat(list(col_exprs))

        hash_expr = concat_expr.transform(_hash_transform, self.source).transform(lower, is_expr=True)

        return build_column(hash_expr, alias=column_alias)
