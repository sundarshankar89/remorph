from databricks.labs.remorph.cli import transpile
from databricks.labs.remorph.snow.databricks import Databricks
from databricks.labs.remorph.teradata import pre_processing_file, post_processing_file

from databricks.labs.remorph.teradata.LLMUtils import convert_using_llm
from databricks.labs.remorph.teradata.FileReadUtil import read_ddl_file
from databricks.labs.remorph.teradata.MainTest import run_remorph_teradata


def run_preprocessor(raw_sql):
    patterns_list, patterns_dict = pre_processing_file.load_and_parse_patterns(
        "/Users/sriram.mohanty/IdeaProjects/remorphTeradata/src/databricks/labs/remorph/teradata/pre_processing_patterns_config.json"
    )
    preprocessed_sql = pre_processing_file.execute(raw_sql, patterns_list, patterns_dict)
    return preprocessed_sql


def run_remorph(preprocessed_sql):
    final_ddl = ""
    try:
        transpiled_sql = transpile(preprocessed_sql, 'teradata', write=Databricks, pretty=True, error_level=None)[0]
        final_ddl = post_processing_file.execute(transpiled_sql)
        print("Post processing-----------")
        print(final_ddl)
    except Exception as e:
        # e.with_traceback()
        final_ddl = preprocessed_sql
        e.with_traceback()
        print("error")
        print(e)
    return final_ddl


def run_llm(final_ddl):
    print("---Calling LLM----")
    convertedSql = convert_using_llm(final_ddl)
    return convertedSql


def write_to_file(content):
    output_file_path = (
        f"/Users/sriram.mohanty/IdeaProjects/remorphTeradata/tests/resources/TeradataConvertedDDL{file_name}"
    )
    with open(output_file_path, 'w') as file:
        file.write(content)


file_name = "/DB_ODS/COUP_LHSALES.ddl"
raw_sql = read_ddl_file(f"/Users/sriram.mohanty/IdeaProjects/remorphTeradata/tests/resources/TeradataDDLs{file_name}")

processed_sql = run_preprocessor(raw_sql)
print(processed_sql)
remorph_ddl = run_remorph_teradata(processed_sql)
print("-----remorph ddl--------")
print(remorph_ddl)
# llm_ddl = run_llm(remorph_ddl)
# print(llm_ddl)
write_to_file(remorph_ddl)
