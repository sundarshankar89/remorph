import logging
from functools import cached_property

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.installer import InstallState
from databricks.labs.blueprint.tui import Prompts
from databricks.labs.blueprint.wheels import ProductInfo
from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config
from databricks.sdk.errors import NotFound
from databricks.sdk.service.iam import User

from databricks.labs.remorph.config import MorphConfig, ReconcileConfig, RemorphConfigs
from databricks.labs.remorph.deployment.configurator import ResourceConfigurator
from databricks.labs.remorph.deployment.dashboard import DashboardDeployment
from databricks.labs.remorph.deployment.installation import WorkspaceInstallation
from databricks.labs.remorph.deployment.recon import TableDeployment, JobDeployment, ReconDeployment
from databricks.labs.remorph.helpers import db_sql
from databricks.labs.remorph.helpers.metastore import CatalogOperations

logger = logging.getLogger(__name__)


class ApplicationContext:
    def __init__(self, ws: WorkspaceClient):
        self._ws = ws

    def replace(self, **kwargs):
        """Replace cached properties for unit testing purposes."""
        for key, value in kwargs.items():
            self.__dict__[key] = value
        return self

    @cached_property
    def workspace_client(self) -> WorkspaceClient:
        return self._ws

    @cached_property
    def current_user(self) -> User:
        return self.workspace_client.current_user.me()

    @cached_property
    def product_info(self) -> ProductInfo:
        return ProductInfo.from_class(RemorphConfigs)

    @cached_property
    def installation(self) -> Installation:
        return Installation.assume_user_home(self.workspace_client, self.product_info.product_name())

    @cached_property
    def transpile_config(self) -> MorphConfig | None:
        try:
            return self.installation.load(MorphConfig)
        except NotFound as err:
            logger.debug(f"Couldn't find existing `transpile` installation: {err}")
            return None

    @cached_property
    def recon_config(self) -> ReconcileConfig | None:
        try:
            return self.installation.load(ReconcileConfig)
        except NotFound as err:
            logger.debug(f"Couldn't find existing `reconcile` installation: {err}")
            return None

    @cached_property
    def remorph_config(self) -> RemorphConfigs:
        return RemorphConfigs(morph=self.transpile_config, reconcile=self.recon_config)

    @cached_property
    def connect_config(self) -> Config:
        return self.workspace_client.config

    @cached_property
    def install_state(self) -> InstallState:
        return InstallState.from_installation(self.installation)

    @cached_property
    def sql_backend(self) -> SqlBackend:
        return db_sql.get_sql_backend(self.workspace_client)

    @cached_property
    def catalog_operations(self) -> CatalogOperations:
        return CatalogOperations(self.workspace_client)

    @cached_property
    def prompts(self) -> Prompts:
        return Prompts()

    @cached_property
    def resource_configurator(self) -> ResourceConfigurator:
        return ResourceConfigurator(self.workspace_client, self.prompts, self.catalog_operations)

    @cached_property
    def table_deployment(self) -> TableDeployment:
        return TableDeployment(self.sql_backend)

    @cached_property
    def job_deployment(self) -> JobDeployment:
        return JobDeployment(self.workspace_client, self.installation, self.install_state, self.product_info)

    @cached_property
    def dashboard_deployment(self) -> DashboardDeployment:
        return DashboardDeployment(self.workspace_client, self.installation, self.install_state)

    @cached_property
    def recon_deployment(self) -> ReconDeployment:
        return ReconDeployment(
            self.workspace_client,
            self.installation,
            self.install_state,
            self.product_info,
            self.table_deployment,
            self.job_deployment,
            self.dashboard_deployment,
        )

    @cached_property
    def workspace_installation(self) -> WorkspaceInstallation:
        return WorkspaceInstallation(
            self.workspace_client,
            self.prompts,
            self.installation,
            self.recon_deployment,
        )
