import logging
import os
from pathlib import Path

from databricks.labs.remorph.__about__ import __version__
from databricks.labs.remorph.config import (
    TranspileConfig,
    TranspileResult,
    ValidationResult,
)
from databricks.labs.remorph.helpers import db_sql
from databricks.labs.remorph.helpers.execution_time import timeit
from databricks.labs.remorph.helpers.file_utils import (
    dir_walk,
    is_sql_file,
    make_dir,
)
from databricks.labs.remorph.reconcile.exception import InvalidInputException
from databricks.labs.remorph.transpiler.transpile_engine import TranspileEngine
from databricks.labs.remorph.transpiler.transpile_status import (
    TranspileStatus,
    ValidationError,
    TranspileError,
    ParserError,
)
from databricks.labs.remorph.helpers.string_utils import remove_bom
from databricks.labs.remorph.helpers.validation import Validator
from databricks.labs.remorph.transpiler.sqlglot.sqlglot_engine import SqlglotEngine
from databricks.sdk import WorkspaceClient

# pylint: disable=unspecified-encoding

logger = logging.getLogger(__name__)


def _process_file(
    config: TranspileConfig,
    validator: Validator | None,
    transpiler: TranspileEngine,
    input_file: Path,
    output_file: Path,
) -> tuple[int, list[TranspileError]]:
    logger.info(f"started processing for the file ${input_file}")
    error_list: list[TranspileError] = []

    with input_file.open("r") as f:
        source_sql = remove_bom(f.read())

    transpile_result = _transpile(
        transpiler, config.source_dialect or "", config.target_dialect, source_sql, input_file
    )
    error_list.extend(transpile_result.error_list)

    with output_file.open("w") as w:
        if validator:
            validation_result = _validation(validator, config, transpile_result.transpiled_code)
            w.write(validation_result.validated_sql)
            if validation_result.exception_msg is not None:
                error_list.append(ValidationError(input_file, validation_result.exception_msg))
        else:
            w.write(transpile_result.transpiled_code)
            w.write("\n;\n")

    return transpile_result.success_count, error_list


def _process_directory(
    config: TranspileConfig,
    validator: Validator | None,
    transpiler: TranspileEngine,
    root: Path,
    base_root: Path,
    files: list[Path],
):
    output_folder = config.output_folder
    output_folder_base = root / ("transpiled" if output_folder is None else base_root)
    make_dir(output_folder_base)

    parse_error_list: list[ParserError] = []
    validate_error_list: list[ValidationError] = []
    counter = 0

    for file in files:
        logger.info(f"Processing file :{file}")
        if not is_sql_file(file):
            continue

        output_file_name = output_folder_base / file.name
        success_count, error_list = _process_file(config, validator, transpiler, file, output_file_name)
        counter = counter + success_count
        parse_error_list.extend([error for error in error_list if isinstance(error, ParserError)])
        validate_error_list.extend([error for error in error_list if isinstance(error, ValidationError)])

    return counter, parse_error_list, validate_error_list


def _process_input_dir(config: TranspileConfig, validator: Validator | None, transpiler: TranspileEngine):
    parse_error_list = []
    validate_error_list = []

    file_list = []
    counter = 0
    input_source = str(config.input_source)
    input_path = Path(input_source)
    for root, _, files in dir_walk(input_path):
        base_root = Path(str(root).replace(input_source, ""))
        folder = str(input_path.resolve().joinpath(base_root))
        msg = f"Processing for sqls under this folder: {folder}"
        logger.info(msg)
        file_list.extend(files)
        no_of_sqls, parse_error, validation_error = _process_directory(
            config, validator, transpiler, root, base_root, files
        )
        counter = counter + no_of_sqls
        parse_error_list.extend(parse_error)
        validate_error_list.extend(validation_error)

    error_log = parse_error_list + validate_error_list

    return TranspileStatus(file_list, counter, len(parse_error_list), len(validate_error_list), error_log)


def _process_input_file(
    config: TranspileConfig, validator: Validator | None, transpiler: TranspileEngine
) -> TranspileStatus:
    if not is_sql_file(config.input_path):
        msg = f"{config.input_source} is not a SQL file."
        logger.warning(msg)
        # silently ignore non-sql files
        return TranspileStatus([], 0, 0, 0, [])
    msg = f"Processing sql from this file: {config.input_source}"
    logger.info(msg)
    if config.output_path is None:
        output_path = config.input_path.parent / "transpiled"
    else:
        output_path = config.output_path

    make_dir(output_path)
    output_file = output_path / config.input_path.name
    no_of_sqls, error_list = _process_file(config, validator, transpiler, config.input_path, output_file)
    parser_errors = [error for error in error_list if isinstance(error, ParserError)]
    validation_errors = [error for error in error_list if isinstance(error, ValidationError)]
    return TranspileStatus([config.input_path], no_of_sqls, len(parser_errors), len(validation_errors), error_list)


@timeit
def transpile(workspace_client: WorkspaceClient, engine: TranspileEngine, config: TranspileConfig):
    """
    [Experimental] Transpiles the SQL queries from one dialect to another.

    :param workspace_client: The WorkspaceClient object.
    :param engine: The TranspileEngine.
    :param config: The configuration for the morph operation.
    """
    if not config.input_source:
        logger.error("Input SQL path is not provided.")
        raise ValueError("Input SQL path is not provided.")

    status = []

    validator = None
    if not config.skip_validation:
        sql_backend = db_sql.get_sql_backend(workspace_client)
        logger.info(f"SQL Backend used for query validation: {type(sql_backend).__name__}")
        validator = Validator(sql_backend)
    if config.input_source is None:
        raise InvalidInputException("Missing input source!")
    if config.input_path.is_dir():
        result = _process_input_dir(config, validator, engine)
    elif config.input_path.is_file():
        result = _process_input_file(config, validator, engine)
    else:
        msg = f"{config.input_source} does not exist."
        logger.error(msg)
        raise FileNotFoundError(msg)

    error_list_count = result.parse_error_count + result.validate_error_count
    if not config.skip_validation:
        logger.info(f"No of Sql Failed while Validating: {result.validate_error_count}")

    error_log_file = "None"
    if error_list_count > 0:
        error_log_file = str(Path.cwd().joinpath(f"err_{os.getpid()}.lst"))
        if result.error_list:
            with Path(error_log_file).open("a") as e:
                e.writelines(f"{err!s}\n" for err in result.error_list)

    status.append(
        {
            "total_files_processed": len(result.file_list),
            "total_queries_processed": result.no_of_transpiled_queries,
            "no_of_sql_failed_while_parsing": result.parse_error_count,
            "no_of_sql_failed_while_validating": result.validate_error_count,
            "error_log_file": str(error_log_file),
        }
    )
    return status


def verify_workspace_client(workspace_client: WorkspaceClient) -> WorkspaceClient:
    # pylint: disable=protected-access
    """
    [Private] Verifies and updates the workspace client configuration.

    TODO: In future refactor this function so it can be used for reconcile module without cross access.
    """
    if workspace_client.config._product != "remorph":
        workspace_client.config._product = "remorph"
    if workspace_client.config._product_version != __version__:
        workspace_client.config._product_version = __version__
    return workspace_client


def _transpile(
    transpiler: TranspileEngine, from_dialect: str, to_dialect: str, source_code: str, input_file: Path
) -> TranspileResult:
    return transpiler.transpile(from_dialect, to_dialect, source_code, input_file)


def _validation(
    validator: Validator,
    config: TranspileConfig,
    sql: str,
) -> ValidationResult:
    return validator.validate_format_result(config, sql)


@timeit
def transpile_sql(
    workspace_client: WorkspaceClient,
    config: TranspileConfig,
    source_sql: str,
) -> tuple[TranspileResult, ValidationResult | None]:
    """[Experimental] Transpile a single SQL query from one dialect to another."""
    ws_client: WorkspaceClient = verify_workspace_client(workspace_client)

    transpiler: TranspileEngine = SqlglotEngine()

    transpiler_result = _transpile(
        transpiler, config.source_dialect or "", config.target_dialect, source_sql, Path("inline_sql")
    )

    if config.skip_validation:
        return transpiler_result, None

    sql_backend = db_sql.get_sql_backend(ws_client)
    logger.info(f"SQL Backend used for query validation: {type(sql_backend).__name__}")
    validator = Validator(sql_backend)
    return transpiler_result, _validation(validator, config, transpiler_result.transpiled_code)


@timeit
def transpile_column_exp(
    workspace_client: WorkspaceClient,
    config: TranspileConfig,
    expressions: list[str],
) -> list[tuple[TranspileResult, ValidationResult | None]]:
    """[Experimental] Transpile a list of SQL expressions from one dialect to another."""
    config.skip_validation = True
    result = []
    for sql in expressions:
        result.append(transpile_sql(workspace_client, config, sql))
    return result
