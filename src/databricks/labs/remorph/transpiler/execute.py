import logging
import os
from pathlib import Path

from databricks.labs.remorph.config import MorphConfig
from databricks.labs.remorph.helpers import db_sql
from databricks.labs.remorph.helpers.execution_time import timeit
from databricks.labs.remorph.helpers.file_utils import (
    dir_walk,
    is_sql_file,
    make_dir,
    remove_bom,
)
from databricks.labs.remorph.helpers.morph_status import MorphStatus, ValidationError
from databricks.labs.remorph.helpers.validation import Validator
from databricks.labs.remorph.snow import lca_utils
from databricks.labs.remorph.snow.sql_transpiler import SqlglotEngine
from databricks.sdk import WorkspaceClient

# pylint: disable=unspecified-encoding

logger = logging.getLogger(__name__)


def process_file(
    config: MorphConfig,
    validator: Validator,
    transpiler: SqlglotEngine,
    input_file: str | Path,
    output_file: str | Path,
):
    logger.info(f"started processing for the file ${input_file}")
    validate_error_list = []
    no_of_sqls = 0

    input_file = Path(input_file)
    output_file = Path(output_file)

    with input_file.open("r") as f:
        sql = remove_bom(f.read())

    lca_error = lca_utils.check_for_unsupported_lca(config.source, sql, str(input_file))

    if lca_error:
        validate_error_list.append(lca_error)

    write_dialect = config.get_write_dialect()

    transpiled_sql, parse_error_list = transpiler.transpile(write_dialect, sql, str(input_file), [])

    with output_file.open("w") as w:
        for output in transpiled_sql:
            if output:
                no_of_sqls = no_of_sqls + 1
                if config.skip_validation:
                    w.write(output)
                    w.write("\n;\n")
                else:
                    output_string, exception = validator.validate_format_result(config, output)
                    w.write(output_string)
                    if exception is not None:
                        validate_error_list.append(ValidationError(str(input_file), exception))
            else:
                warning_message = (
                    f"Skipped a query from file {input_file!s}. "
                    f"Check for unsupported operations related to STREAM, TASK, SESSION etc."
                )
                logger.warning(warning_message)

    return no_of_sqls, parse_error_list, validate_error_list


def process_directory(
    config: MorphConfig,
    validator: Validator,
    transpiler: SqlglotEngine,
    root: str | Path,
    base_root: str,
    files: list[str],
):
    output_folder = config.output_folder
    parse_error_list = []
    validate_error_list = []
    counter = 0

    root = Path(root)

    for file in files:
        logger.info(f"Processing file :{file}")
        if is_sql_file(file):
            if output_folder in {None, "None"}:
                output_folder_base = root / "transpiled"
            else:
                output_folder_base = f'{output_folder.rstrip("/")}/{base_root}'

            output_file_name = Path(output_folder_base) / Path(file).name
            make_dir(output_folder_base)

            no_of_sqls, parse_error, validation_error = process_file(
                config, validator, transpiler, file, output_file_name
            )
            counter = counter + no_of_sqls
            parse_error_list.extend(parse_error)
            validate_error_list.extend(validation_error)
        else:
            # Only SQL files are processed with extension .sql or .ddl
            pass

    return counter, parse_error_list, validate_error_list


def process_recursive_dirs(config: MorphConfig, validator: Validator, transpiler: SqlglotEngine):
    input_sql = Path(config.input_sql)
    parse_error_list = []
    validate_error_list = []

    file_list = []
    counter = 0
    for root, _, files in dir_walk(input_sql):
        base_root = str(root).replace(str(input_sql), "")
        folder = str(input_sql.resolve().joinpath(base_root))
        msg = f"Processing for sqls under this folder: {folder}"
        logger.info(msg)
        file_list.extend(files)
        no_of_sqls, parse_error, validation_error = process_directory(
            config, validator, transpiler, root, base_root, files
        )
        counter = counter + no_of_sqls
        parse_error_list.extend(parse_error)
        validate_error_list.extend(validation_error)

    error_log = parse_error_list + validate_error_list

    return MorphStatus(file_list, counter, len(parse_error_list), len(validate_error_list), error_log)


@timeit
def morph(workspace_client: WorkspaceClient, config: MorphConfig):
    """
    Transpiles the SQL queries from one dialect to another.

    :param config: The configuration for the morph operation.
    :param workspace_client: The WorkspaceClient object.
    """
    input_sql = Path(config.input_sql)
    status = []
    result = MorphStatus([], 0, 0, 0, [])

    read_dialect = config.get_read_dialect()
    transpiler = SqlglotEngine(read_dialect)
    validator = None
    if not config.skip_validation:
        validator = Validator(db_sql.get_sql_backend(workspace_client, config))

    if input_sql.is_file():
        if is_sql_file(input_sql):
            msg = f"Processing for sqls under this file: {input_sql}"
            logger.info(msg)
            if config.output_folder in {None, "None"}:
                output_folder = input_sql.parent / "transpiled"
            else:
                output_folder = Path(config.output_folder.rstrip("/"))

            make_dir(output_folder)
            output_file = output_folder / input_sql.name
            no_of_sqls, parse_error, validation_error = process_file(
                config, validator, transpiler, input_sql, output_file
            )
            error_log = parse_error + validation_error
            result = MorphStatus([str(input_sql)], no_of_sqls, len(parse_error), len(validation_error), error_log)
        else:
            msg = f"{input_sql} is not a SQL file."
            logger.warning(msg)
    elif input_sql.is_dir():
        result = process_recursive_dirs(config, validator, transpiler)
    else:
        msg = f"{input_sql} does not exist."
        logger.error(msg)
        raise FileNotFoundError(msg)

    error_list_count = result.parse_error_count + result.validate_error_count
    if not config.skip_validation:
        logger.info(f"No of Sql Failed while Validating: {result.validate_error_count}")

    error_log_file = "None"
    if error_list_count > 0:
        error_log_file = Path.cwd() / f"err_{os.getpid()}.lst"
        with error_log_file.open("a") as e:
            e.writelines(f"{err}\n" for err in result.error_log_list)

    status.append(
        {
            "total_files_processed": len(result.file_list),
            "total_queries_processed": result.no_of_queries,
            "no_of_sql_failed_while_parsing": result.parse_error_count,
            "no_of_sql_failed_while_validating": result.validate_error_count,
            "error_log_file": str(error_log_file),
        }
    )
    return status
