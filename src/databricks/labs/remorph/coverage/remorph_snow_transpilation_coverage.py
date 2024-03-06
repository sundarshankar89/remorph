from pathlib import Path

from databricks.labs.blueprint.wheels import ProductInfo

from databricks.labs.remorph.coverage import commons
from databricks.labs.remorph.snow.databricks import Databricks
from databricks.labs.remorph.snow.snowflake import Snow

if __name__ == "__main__":
    input_dir = commons.get_env_var("INPUT_DIR", required=True)
    output_dir = commons.get_env_var("OUTPUT_DIR", required=True)

    REMORPH_COMMIT_HASH = commons.get_current_commit_hash() or ""  # C0103 pylint
    product_info = ProductInfo(__file__)
    remorph_version = product_info.unreleased_version()

    commons.collect_transpilation_stats(
        "Remorph",
        REMORPH_COMMIT_HASH,
        remorph_version,
        Snow,
        Databricks,
        Path(input_dir),
        Path(output_dir),
    )
