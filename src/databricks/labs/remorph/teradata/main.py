# Define class to run code_transpiler on teradata objects and execute the corresponding Databricks DDL
from concurrent.futures import ThreadPoolExecutor

from databricks.labs.lsql.structs import StructType, StructField
from pyspark.sql.functions import udf, lit, col
from pyspark.sql.types import StringType, Row

from databricks.labs.remorph.cli import transpile
from databricks.labs.remorph.snow.databricks import Databricks


class DatabricksDDLTranspile:
    def __init__(self, spark, num_threads=1):
        self.spark = spark
        self.num_threads = num_threads

    # Method to pre-process, transpile and post-process Teradata DDL
    @staticmethod
    def transpile_ddl(teradata_sql):
        import pre_processing_file
        import post_processing_file

        # read the config json
        patterns_list, patterns_dict = pre_processing_file.load_and_parse_patterns(
            "/Users/sriram.mohanty/IdeaProjects/remorphFeatureLatest/src/databricks/labs/remorph/teradata/pre_processing_patterns_config.json"
        )
        # patterns_dict = OrderedDict()
        try:
            # Run pre-processing
            sql = pre_processing_file.execute(teradata_sql, patterns_list, patterns_dict)
            # Transpile the DDL
            # transpile is the method imported from the code_transpiler python wheel
            transpiled_sql = transpile(
                "select * form emp", read='teradata', write=Databricks, pretty=True, error_level=None
            )[0]
            # Run post processing
            final_ddl = post_processing_file.execute(transpiled_sql)
            println(final_ddl)
            return final_ddl, None
        except Exception as e:
            e.with_traceback()
            return None, str(e)

    # Convert the transpile function to UDF
    @staticmethod
    def convert_to_udf(function):
        schema = StructType([StructField("transpiled_sql", StringType()), StructField("error_message", StringType())])
        return udf(function, schema)

    # Method to call transpile udf and return transpiled Databricks DDL results
    def generate_databricks_ddl(self, migration_objects_df):
        # Select only the successful objects from the current pass
        migration_objects_df = (
            migration_objects_df.select('db', 'object', 'ddl_output', 'object_type')
            .where("result == 'Success' AND (object_type == 'table' OR object_type == 'view')")
            .dropDuplicates()
        )

        transpile_ddl_udf = self.convert_to_udf(self.transpile_ddl)
        # return transpiled DDL dataframe results
        transpile_df = migration_objects_df.withColumn(
            "transpiled_sql", lit(transpile_ddl_udf(col("ddl_output"))["transpiled_sql"])
        ).withColumn("error_message", lit(transpile_ddl_udf(col("ddl_output"))["error_message"]))
        transpile_df = transpile_df.drop('ddl_output').withColumnRenamed("transpiled_sql", "ddl_output")
        return transpile_df

    # def execute_ddl(self, ddl_statements, catalog_name, sql_db):
    #     result_df = self.spark.createDataFrame([{'ddl_output': ddl_statements, 'db': sql_db}])
    #     try:
    #         self.spark.sql(f"USE CATALOG {catalog_name};")
    #         self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {sql_db}")
    #         for ddl in ddl_statements:
    #             self.spark.sql(ddl)
    #         result_df = result_df.withColumn("error_message", lit(None))
    #     except Exception as e:
    #         result_df = result_df.withColumn("error_message", lit(str(e)))
    #     return result_df

    # Method to run the Databricks DDL in the corresponding destination catalog which will have the migrated objects
    def execute_ddl(self, ddl_statements, catalog, sql_db, sql_object, sql_object_type):
        try:
            self.spark.sql(f"USE CATALOG {catalog};")
            # Create database that will store the migrated objects in databricks
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {sql_db}")
            # iterate over the queries required to successfuly create the object in Databricks
            for ddl in ddl_statements:
                self.spark.sql(ddl)
            error_message = None
        except Exception as e:
            error_message = str(e)
        return Row(
            db=sql_db,
            object=sql_object,
            object_type=sql_object_type,
            ddl_output=ddl_statements,
            error_message=error_message,
        )

    # def run_databricks_ddl(self, row, catalog_name):
    #     sql_db = row['db']
    #     sql_command = row['ddl_output']
    #     sql_ddl_list = sql_command.replace('\n','').split(';')
    #     cleaned_final_ddl = [item.strip() for item in sql_ddl_list if item.strip()]
    #     with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
    #         results = list(executor.map(
    #             lambda ddl: self.execute_ddl([ddl], catalog_name, sql_db),
    #             cleaned_final_ddl
    #         ))
    #     return results

    # Method to extract the DDL output and create them in Databricks
    def run_databricks_ddl(self, rows, catalog):
        # Call execute_ddl in a parallel process to run object creation in parallel
        def execute_ddl_threaded(row):
            sql_db = row['db']
            sql_object = row['object']
            sql_object_type = row['object_type']
            sql_command = row['ddl_output']
            sql_ddl_list = sql_command.replace('\n', ' ').split(';')
            # retrieve list of queries required to execute the object creation
            cleaned_final_ddl = [item.strip() for item in sql_ddl_list if item.strip()]
            # execute ddl for each object
            result = self.execute_ddl(cleaned_final_ddl, catalog, sql_db, sql_object, sql_object_type)
            return result

        results = []
        schema = StructType(
            [
                StructField("db", StringType(), True),
                StructField("object", StringType(), True),
                StructField("object_type", StringType(), True),
                StructField("ddl_output", StringType(), True),
                StructField("error_message", StringType(), True),
            ]
        )
        # Use ThreadPoolExecutor to process object creation in parallel
        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            results = list(executor.map(lambda row: execute_ddl_threaded(row), rows))
        return self.spark.createDataFrame(results, schema)
