from databricks.sdk import WorkspaceClient

from databricks.labs.remorph.config import MorphConfig
from databricks.labs.remorph.transpiler.execute import morph_sql
import requests



def validate_query(query):
    ws = WorkspaceClient(profile='remorph')
    config = MorphConfig(source="snowflake", skip_validation=False, catalog_name="remorph", schema_name="reconcile")
    transpiler_result, validation_result = morph_sql(ws, config, query)
    print(transpiler_result)
    print(validation_result)
    return validation_result.exception_msg


def api_based_validation(query):
    api_token = ("",)
    api_url = ("<workspace_url>/api/2.0",)
    # Set the query to validate
    query = "SELECT * FROM my_table WHERE column = 'value'"

    # Send the validateQuery request
    response = requests.post(
        f"{api_url}/queries/validate", headers={"Authorization": f"Bearer {api_token}"}, json={"query": query}
    )

    # Check the response
    if response.status_code == 200:
        print("Query is syntactically correct")
    else:
        print("Query has syntax errors")
    api_based_validation("")
