import logging

from sqlglot import expressions as exp
from sqlglot import select

from databricks.labs.remorph.reconcile.constants import ThresholdMode
from databricks.labs.remorph.reconcile.query_builder.base import QueryBuilder
from databricks.labs.remorph.reconcile.query_builder.expression_generator import (
    anonymous,
    build_between,
    build_column,
    build_from_clause,
    build_if,
    build_join_clause,
    build_literal,
    build_sub,
    build_where_clause,
    coalesce,
)
from databricks.labs.remorph.reconcile.recon_config import Thresholds
from databricks.labs.remorph.snow.databricks import Databricks

logger = logging.getLogger(__name__)


class ThresholdQueryBuilder(QueryBuilder):
    # Comparison query
    def build_comparison_query(self) -> str:
        select_clause, where = self._generate_select_where_clause()
        from_clause, join_clause = self._generate_from_and_join_clause()
        # for threshold comparison query the dialect is always Daabricks
        query = select(*select_clause).from_(from_clause).join(join_clause).where(where).sql(dialect=Databricks)
        logger.info(f"Threshold Comparison query: {query}")
        return query

    def _generate_select_where_clause(self) -> tuple[list[exp.Alias], exp.Or]:
        thresholds = self.table_conf.thresholds
        select_clause = []
        where_clause = []

        # threshold columns
        for threshold in thresholds:
            column = threshold.column_name
            base = exp.Paren(
                this=build_sub(
                    left_column_name=column,
                    left_table_name="source",
                    right_column_name=column,
                    right_table_name="databricks",
                )
            ).transform(coalesce)

            select_exp, where = self._build_expression_type(threshold, base)
            select_clause.extend(select_exp)
            where_clause.append(where)
        # join columns
        for column in sorted(self.table_conf.get_join_columns("source")):
            select_clause.append(build_column(this=column, alias=f"{column}_source", table_name="source"))
        where = build_where_clause(where_clause)

        return select_clause, where

    @classmethod
    def _build_expression_alias_components(
        cls, threshold: Thresholds, base: exp.Expression
    ) -> tuple[list[exp.Alias], exp.Expression]:
        select_clause = []
        column = threshold.column_name
        select_clause.append(
            build_column(this=column, alias=f"{column}_source", table_name="source").transform(coalesce)
        )
        select_clause.append(
            build_column(this=column, alias=f"{column}_databricks", table_name="databricks").transform(coalesce)
        )
        where_clause = exp.NEQ(this=base, expression=exp.Literal(this="0", is_string=False))
        return select_clause, where_clause

    def _build_expression_type(
        self, threshold: Thresholds, base: exp.Expression
    ) -> tuple[list[exp.Alias], exp.Expression]:
        column = threshold.column_name
        # default expressions
        select_clause, where_clause = self._build_expression_alias_components(threshold, base)

        if threshold.get_type() in (ThresholdMode.NUMBER_ABSOLUTE.value, ThresholdMode.DATETIME.value):
            if threshold.get_type() == ThresholdMode.DATETIME.value:
                # unix_timestamp expression only if it is datetime
                select_clause = [expression.transform(anonymous, "unix_timestamp({})") for expression in select_clause]
                base = base.transform(anonymous, "unix_timestamp({})")
                where_clause = exp.NEQ(this=base, expression=exp.Literal(this="0", is_string=False))

            # absolute threshold
            func = self._build_threshold_absolute_case
        elif threshold.get_type() == ThresholdMode.NUMBER_PERCENTAGE.value:
            # percentage threshold
            func = self._build_threshold_percentage_case
        else:
            error_message = f"Threshold type {threshold.get_type()} not supported for column {column}"
            logger.error(error_message)
            raise ValueError(error_message)

        select_clause.append(build_column(this=func(base=base, threshold=threshold), alias=f"{column}_match"))

        return select_clause, where_clause

    def _generate_from_and_join_clause(self) -> tuple[exp.From, exp.Join]:
        join_columns = sorted(self.table_conf.get_join_columns("source"))
        source_view = f"{self.table_conf.source_name}_df_threshold_vw"
        target_view = f"{self.table_conf.target_name}_df_threshold_vw"

        from_clause = build_from_clause(source_view, "source")
        join_clause = build_join_clause(
            table_name=target_view,
            source_table_alias="source",
            target_table_alias="databricks",
            join_columns=join_columns,
        )

        return from_clause, join_clause

    @classmethod
    def _build_threshold_absolute_case(cls, base: exp.Expression, threshold: Thresholds) -> exp.Case:
        eq_if = build_if(
            this=exp.EQ(this=base, expression=build_literal(this="0", is_string=False)),
            true=exp.Identifier(this="Match", quoted=True),
        )

        between_base = build_between(
            this=base,
            low=build_literal(threshold.lower_bound.replace("%", ""), is_string=False),
            high=build_literal(threshold.upper_bound.replace("%", ""), is_string=False),
        )

        between_if = build_if(
            this=between_base,
            true=exp.Identifier(this="Warning", quoted=True),
        )
        return exp.Case(ifs=[eq_if, between_if], default=exp.Identifier(this="Failed", quoted=True))

    @classmethod
    def _build_threshold_percentage_case(cls, base: exp.Expression, threshold: Thresholds) -> exp.Case:
        eq_if = exp.If(
            this=exp.EQ(this=base, expression=build_literal(this="0", is_string=False)),
            true=exp.Identifier(this="Match", quoted=True),
        )

        denominator = build_if(
            this=exp.Or(
                this=exp.EQ(
                    this=exp.Column(this=threshold.column_name, table="databricks"),
                    expression=exp.Literal(this='0', is_string=False),
                ),
                expression=exp.Is(
                    this=exp.Column(
                        this=exp.Identifier(this=threshold.column_name, quoted=False),
                        table=exp.Identifier(this='databricks'),
                    ),
                    expression=exp.Null(),
                ),
            ),
            true=exp.Literal(this="1", is_string=False),
            false=exp.Column(this=threshold.column_name, table="databricks"),
        )

        division = exp.Div(this=base, expression=denominator, typed=False, safe=False)
        percentage = exp.Mul(this=exp.Paren(this=division), expression=exp.Literal(this="100", is_string=False))
        between_base = build_between(
            this=percentage,
            low=build_literal(threshold.lower_bound.replace("%", ""), is_string=False),
            high=build_literal(threshold.upper_bound.replace("%", ""), is_string=False),
        )

        between_if = build_if(
            this=between_base,
            true=exp.Identifier(this="Warning", quoted=True),
        )
        return exp.Case(ifs=[eq_if, between_if], default=exp.Identifier(this="Failed", quoted=True))
