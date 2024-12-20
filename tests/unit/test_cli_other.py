from unittest.mock import patch


from databricks.labs.blueprint.tui import MockPrompts
from databricks.labs.remorph import cli
from databricks.labs.remorph.helpers.recon_config_utils import ReconConfigPrompts


def test_configure_secrets_databricks(mock_workspace_client):
    source_dict = {"databricks": "0", "netezza": "1", "oracle": "2", "snowflake": "3"}
    prompts = MockPrompts(
        {
            r"Select the source": source_dict["databricks"],
        }
    )

    recon_conf = ReconConfigPrompts(mock_workspace_client, prompts)
    recon_conf.prompt_source()

    recon_conf.prompt_and_save_connection_details()


def test_cli_configure_secrets_config(mock_workspace_client):
    with patch("databricks.labs.remorph.cli.ReconConfigPrompts") as mock_recon_config:
        cli.configure_secrets(mock_workspace_client)
        mock_recon_config.assert_called_once_with(mock_workspace_client)


def test_cli_reconcile(mock_workspace_client):
    with patch("databricks.labs.remorph.reconcile.runner.ReconcileRunner.run", return_value=True):
        cli.reconcile(mock_workspace_client)


def test_cli_aggregates_reconcile(mock_workspace_client):
    with patch("databricks.labs.remorph.reconcile.runner.ReconcileRunner.run", return_value=True):
        cli.aggregates_reconcile(mock_workspace_client)
