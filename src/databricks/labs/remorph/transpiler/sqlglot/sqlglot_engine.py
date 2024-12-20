import logging
import typing as t
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from sqlglot import expressions as exp, parse, transpile, Dialect
from sqlglot.errors import ErrorLevel, ParseError, TokenError, UnsupportedError
from sqlglot.expressions import Expression
from sqlglot.tokens import Token, TokenType

from databricks.labs.remorph.config import TranspileResult
from databricks.labs.remorph.helpers.string_utils import format_error_message
from databricks.labs.remorph.transpiler.sqlglot import lca_utils
from databricks.labs.remorph.transpiler.sqlglot.dialect_utils import get_dialect
from databricks.labs.remorph.transpiler.sqlglot.dialect_utils import SQLGLOT_DIALECTS
from databricks.labs.remorph.transpiler.transpile_status import ParserError, ValidationError
from databricks.labs.remorph.transpiler.transpile_engine import TranspileEngine

logger = logging.getLogger(__name__)


@dataclass
class ParsedExpression:
    original_sql: str
    parsed_expression: Expression


@dataclass
class ParserProblem:
    original_sql: str
    parser_error: ParserError


class SqlglotEngine(TranspileEngine):

    @property
    def supported_dialects(self) -> list[str]:
        return sorted(SQLGLOT_DIALECTS.keys())

    def _partial_transpile(
        self,
        read_dialect: Dialect,
        write_dialect: Dialect,
        source_code: str,
        file_path: Path,
    ) -> tuple[list[str], list[ParserProblem]]:
        transpiled_sqls: list[str] = []
        parsed_expressions, problem_list = self.safe_parse(read_dialect, source_code, file_path)
        for parsed_expression in parsed_expressions:
            try:
                transpiled_sql = write_dialect.generate(parsed_expression.parsed_expression, pretty=True)
                # Checking if the transpiled SQL is a comment and raise an error
                if transpiled_sql.startswith("--"):
                    raise UnsupportedError("Unsupported SQL")
                transpiled_sqls.append(transpiled_sql)
            except ParseError as e:
                error_msg = format_error_message("Parsing Error", e, parsed_expression.original_sql)
                problem_list.append(ParserProblem(parsed_expression.original_sql, ParserError(file_path, error_msg)))
            except UnsupportedError as e:
                error_msg = format_error_message("Unsupported SQL Error", e, parsed_expression.original_sql)
                problem_list.append(ParserProblem(parsed_expression.original_sql, ParserError(file_path, error_msg)))
            except TokenError as e:
                error_msg = format_error_message("Token Error", e, parsed_expression.original_sql)
                problem_list.append(ParserProblem(parsed_expression.original_sql, ParserError(file_path, error_msg)))
        return transpiled_sqls, problem_list

    def transpile(self, source_dialect: str, target_dialect: str, source_code: str, file_path: Path) -> TranspileResult:
        read_dialect = get_dialect(source_dialect)
        write_dialect = get_dialect(target_dialect)
        try:
            transpiled_expressions = transpile(
                source_code, read=read_dialect, write=write_dialect, pretty=True, error_level=ErrorLevel.RAISE
            )
            transpiled_code = "\n".join(transpiled_expressions)
            return TranspileResult(transpiled_code, len(transpiled_expressions), [])
        except (ParseError, TokenError, UnsupportedError) as e:
            logger.error(f"Exception caught for file {file_path!s}: {e}")
            transpiled_expressions, problems = self._partial_transpile(
                read_dialect, write_dialect, source_code, file_path
            )
            transpiled_code = "\n".join(transpiled_expressions)
            return TranspileResult(transpiled_code, 1, [problem.parser_error for problem in problems])

    def parse(
        self, source_dialect: str, source_sql: str, file_path: Path
    ) -> tuple[list[Expression | None] | None, ParserError | None]:
        expression = None
        error = None
        try:
            expression = parse(source_sql, read=source_dialect, error_level=ErrorLevel.IMMEDIATE)
        except (ParseError, TokenError, UnsupportedError) as e:
            error = ParserError(file_path, str(e))

        return expression, error

    def analyse_table_lineage(
        self, source_dialect: str, source_code: str, file_path: Path
    ) -> Iterable[tuple[str, str]]:
        parsed_expression, _ = self.parse(source_dialect, source_code, file_path)
        if parsed_expression is not None:
            for expr in parsed_expression:
                child: str = str(file_path)
                if expr is not None:
                    # TODO: fix possible issue where the file reference is lost (if we have a 'create')
                    for change in expr.find_all(exp.Create, exp.Insert, exp.Merge, bfs=False):
                        child = self._find_root_table(change)

                    for query in expr.find_all(exp.Select, exp.Join, exp.With, bfs=False):
                        table = self._find_root_table(query)
                        if table:
                            yield table, child

    def safe_parse(
        self, read_dialect: Dialect, source_code: str, file_path: Path
    ) -> tuple[list[ParsedExpression], list[ParserProblem]]:
        try:
            tokens = read_dialect.tokenize(sql=source_code)
            return self._safe_parse(read_dialect, tokens, file_path)
        except TokenError as e:
            error_msg = format_error_message("TOKEN ERROR", e, source_code)
            return [], [ParserProblem(source_code, ParserError(file_path=file_path, error_msg=error_msg))]

    def _safe_parse(
        self, read_dialect: Dialect, all_tokens: list[Token], file_path: Path
    ) -> tuple[list[ParsedExpression], list[ParserProblem]]:
        chunks = self._make_chunks(all_tokens)
        parsed_expressions: list[ParsedExpression] = []
        problems: list[ParserProblem] = []
        parser_opts = {"error_level": ErrorLevel.RAISE}
        parser = read_dialect.parser(**parser_opts)
        for sql, tokens in chunks:
            try:
                expressions = parser.parse(tokens)
                expression = t.cast(Expression, expressions[0])
                parsed_expressions.append(ParsedExpression(sql, expression))
            except (ParseError, TokenError, UnsupportedError) as e:
                error_msg = format_error_message("PARSING ERROR", e, sql)
                problems.append(ParserProblem(sql, ParserError(file_path, error_msg)))
            finally:
                parser.reset()
        return parsed_expressions, problems

    @staticmethod
    def _make_chunks(tokens: list[Token]) -> list[tuple[str, list[Token]]]:
        chunks: list[tuple[str, list[Token]]] = []
        current_chunk: list[Token] = []
        # Split tokens into chunks based on semicolons(or other separators)
        # Need to define the separator in Class Tokenizer
        for token in tokens:
            current_chunk.append(token)
            if token.token_type in {TokenType.SEMICOLON}:
                original_sql = " ".join([token.text for token in current_chunk]).strip()
                chunks.append((original_sql, current_chunk))
                # reset
                current_chunk = []
        # don't forget the last chunk
        if current_chunk:
            original_sql = " ".join([token.text for token in current_chunk]).strip()
            chunks.append((original_sql, current_chunk))
        return chunks

    @staticmethod
    def _find_root_table(expression) -> str:
        table = expression.find(exp.Table, bfs=False)
        return table.name if table else ""

    def check_for_unsupported_lca(self, source_dialect, source_code, file_path) -> ValidationError | None:
        return lca_utils.check_for_unsupported_lca(get_dialect(source_dialect), source_code, file_path)
