from dspy import TypedChainOfThought

import dspy


def convert_using_llm(query):
    client = dspy.Databricks(
        model="databricks-meta-llama-3-1-70b-instruct",
        api_key="",
        api_base="https://e2-demo-west.cloud.databricks.com/serving-endpoints",
        model_type="chat",
        max_tokens=5000,
    )
    dspy.settings.configure(lm=client)
    tc = TeradataConverter()
    result = tc(input_string=query)
    return result


class QueryConverter(dspy.Signature):
    """
    Signature for converting Teradata queries to Databricks SQL.Make sure data types are converted equivalently.
    Make sure CHAR is converted to CHAR, varchar is converted to varchar, decimal is converted to decimal, etc.
    Make sure table name is intact and column names are intact.
    """

    teradata_query: str = dspy.InputField(desc="Input string containing a teradata DDL query")
    databricks_query: str = dspy.OutputField(
        desc="Equivalent Databricks DDL query table format should be DELTA. Make sure column type are converted correctly ex:Make sure CHAR is converted to CHAR, varchar is converted to varchar, decimal is converted to decimal, etc.,result should contain only query."
    )


class TeradataConverter(dspy.Module):
    def __init__(self):
        super().__init__()
        self.query_converter = TypedChainOfThought(QueryConverter, max_retries=10)

    def forward(self, input_string: str) -> str:
        return self.query_converter(teradata_query=input_string).databricks_query
