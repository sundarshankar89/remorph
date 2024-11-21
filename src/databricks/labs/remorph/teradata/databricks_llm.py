from dspy import TypedChainOfThought

import dspy


client = dspy.Databricks(
    model="databricks-meta-llama-3-1-70b-instruct",
    api_key="",
    api_base="<workspace_url>/serving-endpoints/databricks-meta-llama-3-1-70b-instruct/invocations",
    model_type="text",
    max_tokens=5000,
)

dspy.settings.configure(lm=client)
class QueryConverter(dspy.Signature):
    """
    Signature for converting Teradata queries to Databricks SQL
    """

    teradata_query: str = dspy.InputField(desc="Input string containing a teradata DDL query")
    databricks_query: str = dspy.OutputField(desc="Equivalent Databricks DDL query")


class TeradataConverter(dspy.Module):
    def __init__(self):
        super().__init__()
        self.query_converter = TypedChainOfThought(QueryConverter, max_retries=10)

    def forward(self, input_string: str) -> str:
        return self.query_converter(teradata_query=input_string).databricks_query


def convert_teradata_to_databricks(teradata_query):
    tc = TeradataConverter()
    result = tc(input_string=teradata_query)
    print(result)
    return result


# Sample Teradata query
teradata_query = "SELECT * FROM my_table WHERE column1 = 'value1' AND column2 = 'value2'"
# Convert the Teradata query to Databricks SQL
databricks_query = convert_teradata_to_databricks(teradata_query)
print(databricks_query)
