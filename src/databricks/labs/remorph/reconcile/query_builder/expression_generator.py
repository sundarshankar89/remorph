from collections.abc import Callable
from functools import partial

from sqlglot import expressions as exp

from databricks.labs.remorph.reconcile.recon_config import DialectHashConfig


def _apply_func_expr(expr: exp.Expression, expr_func: Callable, **kwargs) -> exp.Expression:
    is_terminal = isinstance(expr, exp.Column)
    new_expr = expr.copy()
    for node in new_expr.dfs():
        if isinstance(node, exp.Column):
            column_name = node.name
            table_name = node.table
            func = expr_func(this=exp.Column(this=column_name, table=table_name), **kwargs)
            if is_terminal:
                return func
            node.replace(func)
    return new_expr


def concat(expr: list[exp.Expression]) -> exp.Expression:
    return exp.Concat(expressions=expr, safe=True)


def sha2(expr: exp.Expression, num_bits: str, is_expr: bool = False) -> exp.Expression:
    if is_expr:
        return exp.SHA2(this=expr, length=exp.Literal(this=num_bits, is_string=False))
    return _apply_func_expr(expr, exp.SHA2, length=exp.Literal(this=num_bits, is_string=False))


def lower(expr: exp.Expression, is_expr: bool = False) -> exp.Expression:
    if is_expr:
        return exp.Lower(this=expr)
    return _apply_func_expr(expr, exp.Lower)


def coalesce(expr: exp.Expression, default="0", is_string=False) -> exp.Coalesce | exp.Expression:
    expressions = [exp.Literal(this=default, is_string=is_string)]
    return _apply_func_expr(expr, exp.Coalesce, expressions=expressions)


def trim(expr: exp.Expression) -> exp.Trim | exp.Expression:
    return _apply_func_expr(expr, exp.Trim)


def json_format(expr: exp.Expression, options: dict[str, str] | None = None) -> exp.JSONFormat | exp.Expression:
    return _apply_func_expr(expr, exp.JSONFormat, options=options)


def sort_array(expr: exp.Expression, asc=True):
    return _apply_func_expr(expr, exp.SortArray, asc=exp.Boolean(this=asc))


def to_char(expr: exp.Expression, to_format=None, nls_param=None):
    if to_format:
        return _apply_func_expr(
            expr, exp.ToChar, format=exp.Literal(this=to_format, is_string=True), nls_param=nls_param
        )
    return _apply_func_expr(expr, exp.ToChar)


def array_to_string(
    expr: exp.Expression,
    delimiter: str = ",",
    is_string=True,
    null_replacement: str | None = None,
    is_null_replace=True,
):
    if null_replacement:
        return _apply_func_expr(
            expr,
            exp.ArrayToString,
            expression=[exp.Literal(this=delimiter, is_string=is_string)],
            null=exp.Literal(this=null_replacement, is_string=is_null_replace),
        )
    return _apply_func_expr(expr, exp.ArrayToString, expression=[exp.Literal(this=delimiter, is_string=is_string)])


def array_sort(expr: exp.Expression, asc=True):
    return _apply_func_expr(expr, exp.ArraySort, expression=exp.Boolean(this=asc))


def anonymous(expr: exp.Column, func: str, is_expr: bool = False) -> exp.Anonymous | exp.Expression:
    """

    This function used in cases where the sql functions are not available in sqlGlot expressions
    Example:
        >>> from sqlglot import parse_one
        >>> print(repr(parse_one('select unix_timestamp(col1)')))

    the above code gives you a Select Expression of Anonymous function.

    To achieve the same,we can use the function as below:
    eg:
        >>> expr = parse_one("select col1 from dual")
        >>> transformed_expr=anonymous(expr,"unix_timestamp({})")
        >>> print(transformed_expr)
        'SELECT UNIX_TIMESTAMP(col1) FROM DUAL'

    """
    if is_expr:
        return exp.Column(this=func.format(expr))
    is_terminal = isinstance(expr, exp.Column)
    new_expr = expr.copy()
    for node in new_expr.dfs():
        if isinstance(node, exp.Column):
            name = f"{node.table}.{node.name}" if node.table else node.name
            anonymous_func = exp.Column(this=func.format(name))
            if is_terminal:
                return anonymous_func
            node.replace(anonymous_func)
    return new_expr


def build_column(this: exp.ExpOrStr, table_name="", quoted=False, alias=None) -> exp.Alias | exp.Column:
    if alias:
        if isinstance(this, str):
            return exp.Alias(
                this=exp.Column(this=this, table=table_name), alias=exp.Identifier(this=alias, quoted=quoted)
            )
        return exp.Alias(this=this, alias=exp.Identifier(this=alias, quoted=quoted))
    return exp.Column(this=exp.Identifier(this=this, quoted=quoted), table=table_name)


def build_literal(this: exp.ExpOrStr, alias=None, quoted=False, is_string=True) -> exp.Alias | exp.Literal:
    if alias:
        return exp.Alias(
            this=exp.Literal(this=this, is_string=is_string), alias=exp.Identifier(this=alias, quoted=quoted)
        )
    return exp.Literal(this=this, is_string=is_string)


def transform_expression(
    expr: exp.Expression,
    funcs: list[Callable[[exp.Expression], exp.Expression]],
) -> exp.Expression:
    for func in funcs:
        expr = func(expr)
    assert isinstance(expr, exp.Expression), (
        f"Func returned an instance of type [{type(expr)}], " "should have been Expression."
    )
    return expr


def get_hash_transform(source: str):
    dialect_algo = list(filter(lambda dialect: dialect.dialect == source, Dialect_hash_algo_mapping))
    if dialect_algo:
        return dialect_algo[0].algo
    raise ValueError(f"Source {source} is not supported")


def build_from_clause(table_name: str, table_alias: str | None = None) -> exp.From:
    return exp.From(this=exp.Table(this=exp.Identifier(this=table_name), alias=table_alias))


def build_join_clause(
    table_name: str,
    join_columns: list,
    source_table_alias: str | None = None,
    target_table_alias: str | None = None,
    kind: str = "inner",
) -> exp.Join:
    join_conditions = []
    for column in join_columns:
        join_condition = exp.NullSafeEQ(
            this=exp.Column(this=column, table=source_table_alias),
            expression=exp.Column(this=column, table=target_table_alias),
        )
        join_conditions.append(join_condition)

    # Combine all join conditions with AND
    on_condition = join_conditions[0]
    for condition in join_conditions[1:]:
        on_condition = exp.And(this=on_condition, expression=condition)

    return exp.Join(
        this=exp.Table(this=exp.Identifier(this=table_name), alias=target_table_alias), kind=kind, on=on_condition
    )


def build_sub(
    left_column_name: str,
    right_column_name: str,
    left_table_name: str | None = None,
    right_table_name: str | None = None,
) -> exp.Sub:
    return exp.Sub(
        this=build_column(left_column_name, left_table_name),
        expression=build_column(right_column_name, right_table_name),
    )


def build_where_clause(where_clause=list[exp.Expression], condition_type: str = "or") -> exp.Or:
    func = exp.Or if condition_type == "or" else exp.And
    # Start with a default
    combined_expression = exp.Paren(this=func(this='1 = 1', expression='1 = 1'))

    # Loop through the expressions and combine them with OR
    for expression in where_clause:
        combined_expression = func(this=combined_expression, expression=expression)

    return combined_expression


def build_if(this: exp.Expression, true: exp.Expression, false: exp.Expression | None = None) -> exp.If:
    return exp.If(this=this, true=true, false=false)


def build_between(this: exp.Expression, low: exp.Expression, high: exp.Expression) -> exp.Between:
    return exp.Between(this=this, low=low, high=high)


DataType_transform_mapping = {
    "default": [partial(coalesce, default='', is_string=True), trim],
    "snowflake": {exp.DataType.Type.ARRAY.value: [array_to_string, array_sort]},
    "oracle": {
        exp.DataType.Type.NCHAR.value: [partial(anonymous, func="NVL(TRIM(TO_CHAR({})),'_null_recon_')")],
        exp.DataType.Type.NVARCHAR.value: [partial(anonymous, func="NVL(TRIM(TO_CHAR({})),'_null_recon_')")],
    },
    "databricks": {
        exp.DataType.Type.ARRAY.value: [partial(anonymous, func="CONCAT_WS(',', SORT_ARRAY({}))")],
    },
}

Dialect_hash_algo_mapping = [
    DialectHashConfig(dialect="snowflake", algo=[partial(sha2, num_bits="256", is_expr=True)]),
    DialectHashConfig(
        dialect="oracle", algo=[partial(anonymous, func="RAWTOHEX(STANDARD_HASH({}, 'SHA256'))", is_expr=True)]
    ),
    DialectHashConfig(dialect="databricks", algo=[partial(sha2, num_bits="256", is_expr=True)]),
]
