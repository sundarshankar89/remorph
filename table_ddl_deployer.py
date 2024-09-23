import glob
import json
from pathlib import Path

from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from pyspark.errors import ParseException, AnalysisException

from databricks.labs.remorph.transpiler.execute import morph

from src.databricks.labs.remorph.config import MorphConfig
from src.databricks.labs.remorph.helpers.file_utils import dir_walk


def extract_db_table_name(sql: str):
    import re

    data = [line for line in sql.split("\n") if "CREATE" in line][0]
    result = re.sub(r'CREATE (SET|MULTISET) TABLE\s*|\s*', '', data)
    tbl = result.split(",")[0]
    # print(tbl)
    return tbl


def generate_split():
    # input_file = "../sm/sample.sql"
    input_file = "../test/final.sql"
    split_out = "../final_split"
    with open(input_file, "r") as f:
        sql = f.read().strip()
        tbl_list = []
        for query in sql.split(";"):
            if query:
                query = query.strip()
                tbl_name = extract_db_table_name(query)
                # tbl_list.append(tbl_name)
                # print(tbl_list)
                with open(split_out + f"/{tbl_name}.sql", "a") as w:
                    w.write("\n;\n")
                    w.write(query)


def validate_query(query: str):

    try:
        # [TODO]: When variables is mentioned Explain fails we need way to replace them before explain is executed.
        # [TODO]: Explain needs to redirected to different console
        # [TODO]: Hacky way to replace variables representation
        # ws = WorkspaceClient()
        # spark = DatabricksSession.builder.getOrCreate()
        #
        # spark.sql("create catalog if not exists transpiler_test")
        # spark.sql("use catalog transpiler_test")
        # spark.sql("create schema if not exists convertor_test")
        # spark.sql("use convertor_test")
        ws = WorkspaceClient()
        spark = DatabricksSession.builder.getOrCreate()
        spark.sql("use catalog remorph")

        spark.sql(query.replace("${", "`{").replace("}", "}`").replace("``", "`")).explain(True)
        return True, str("Success")
    except ParseException as pe:
        print("Syntax Exception : NOT IGNORED. Flag as syntax error :" + str(pe))
        return False, str(pe)
    except AnalysisException as aex:
        # https://github.com/databricks/runtime/blob/23ad7b3528766fc85967b63216aad255469b27f8/docs/sql-error-conditions.md
        if "[TABLE_OR_VIEW_NOT_FOUND]" in str(aex):
            print("Analysis Exception : IGNORED: " + str(aex))
            return True, str(aex)
        elif "[TABLE_OR_VIEW_ALREADY_EXISTS]" in str(aex):
            print("Analysis Exception : IGNORED: " + str(aex))
            return True, str(aex)
        elif "[SCHEMA_NOT_FOUND]" in str(aex):
            print("Analysis Exception : IGNORED: " + str(aex))
            return True, str(aex)
        elif "[UNRESOLVED_ROUTINE]" in str(aex):
            print("Analysis Exception : NOT IGNORED: Flag as Function Missing error" + str(aex))
            return False, str(aex)
        elif "Hive support is required to CREATE Hive TABLE (AS SELECT).;" in str(aex):
            print("Analysis Exception : IGNORED: " + str(aex))
            return True, str(aex)
        else:
            print("Unknown Exception: " + str(aex))
            return False, str(aex)
    except Exception as e:
        print("Other Exception : NOT IGNORED. Flagged :" + str(e))
        return False, str(e)


def create_schemas(dir_path_str: str):

    dir_path = Path(dir_path_str)

    schema_list = []
    for root, _, files in dir_walk(dir_path):
        base_root = str(root).replace(str(dir_path), "")
        folder = str(dir_path.resolve().joinpath(base_root))
        msg = f"Processing for sqls under this folder: {folder}"
        print(msg)
        for file in files:
            schema_list.append(str(file).replace(dir_path_str + "/", '').split("/")[1])

    schemas = set(schema_list)
    print(schemas, len(schemas))

    # schemas = set(item.lstrip(dir_path + "/").rstrip(".sql").split(".")[0] for item in glob.glob(dir_path + "/*.sql"))
    # print(schemas)
    # ws = WorkspaceClient()
    spark = DatabricksSession.builder.getOrCreate()
    for s in schemas:
        print(s)
        spark.sql(f"create schema if not exists remorph.{s}")


def run_validate(dir_path: str):
    for file_path in glob.glob(dir_path + "/*.sql"):
        file_name = file_path.lstrip(dir_path + "/").rstrip(".sql")
        print(f"Processing file...{file_name}")
        with open(file_path, "r") as f:
            sql = f.read()
            res, res_str = validate_query(sql)
            if not res:
                print(file_path, res)


def run_ddl_creation(dir_path_str: str):
    spark = DatabricksSession.builder.getOrCreate()

    for file_path in glob.glob(f"{dir_path_str}/**/*.ddl", recursive=True):
        file_name = file_path.lstrip(dir_path_str + "/").rstrip(".ddl")
        print(f"Processing file: {file_name}")
        with open(file_path, "r") as f:
            ddl_query = f.read()
            try:
                # print(ddl_query)
                spark.sql("use catalog remorph")
                spark.sql(ddl_query)
            except Exception as e:
                print(e)


def run_morph(input_sql: str, out_dir: str):

    w = WorkspaceClient()
    config = MorphConfig(
        source="teradata",
        input_sql=input_sql,
        output_folder=out_dir,
        skip_validation=True,
    )

    status = morph(w, config)

    print(json.dumps(status))


def errors_list(file_path: str):
    # Parse errors in errors list
    with open(file_path, "r") as f:
        lines = f.read().strip().split("\n")
        # print(len(lines))
        for index, line in enumerate(lines, start=1):
            # print(f"Processing {index}.. {line}")
            formatted_line = line.replace("\\n", '')
            obj = eval(formatted_line)
            # print(type(obj), obj.exception)
            print(f"'{obj.file_name.strip('td_scripts/')},'")


def run_queries():
    input_sql = "td_Scripts"
    out_dir = "td_out1"
    error_file_path = "new_err_83326.lst"

    # run_morph(input_sql, out_dir)
    # create_schemas(out_dir)
    run_ddl_creation(out_dir)
    # errors_list(error_file_path)


run_queries()
