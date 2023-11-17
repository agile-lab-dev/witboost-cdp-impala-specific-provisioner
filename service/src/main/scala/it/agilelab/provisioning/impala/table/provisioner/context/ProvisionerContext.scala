package it.agilelab.provisioning.impala.table.provisioner.context

import cats.implicits.toBifunctorOps
import it.agilelab.provisioning.commons.client.cdp.dl.CdpDlClient
import it.agilelab.provisioning.commons.client.cdp.env.CdpEnvClient
import it.agilelab.provisioning.commons.config.Conf
import it.agilelab.provisioning.impala.table.provisioner.context.ContextError.{
  ClientError,
  ConfigurationError
}

final case class ProvisionerContext(
    deployRoleUser: String,
    deployRolePwd: String,
    cdpEnvClient: CdpEnvClient,
    cdpDlClient: CdpDlClient
)

object ProvisionerContext {

  private val CDP_DEPLOY_ROLE_USER = "CDP_DEPLOY_ROLE_USER"
  private val CDP_DEPLOY_ROLE_PASSWORD = "CDP_DEPLOY_ROLE_PASSWORD"
  // Unused variables since we're not implementing Provisioning Status (async requests) and role mapping with DB yet
  // To understand how these were used, check the original CDP project repository (pre CDP-refresh)
  private val PROVISION_STATE_TABLE = "PROVISION_STATE_TABLE"
  private val PROVISION_STATE_TABLE_KEY = "PROVISION_STATE_TABLE_KEY"
  private val DATA_MESH_ROLE_TABLE = "DATA_MESH_ROLE_TABLE"
  private val DATA_MESH_ROLE_TABLE_KEY = "DATA_MESH_ROLE_TABLE_KEY"

  def init(
      conf: Conf
  ): Either[ContextError, ProvisionerContext] =
    for {
      deployRoleUser <- conf.get(CDP_DEPLOY_ROLE_USER).leftMap(e => ConfigurationError(e))
      deployRolePwd  <- conf.get(CDP_DEPLOY_ROLE_PASSWORD).leftMap(e => ConfigurationError(e))
      cdpEnvClient   <- CdpEnvClient.defaultWithAudit().leftMap(e => ClientError("CdpEnvClient", e))
      cdpDlClient    <- CdpDlClient.defaultWithAudit().leftMap(e => ClientError("CdpDlClient", e))
    } yield ProvisionerContext(
      deployRoleUser,
      deployRolePwd,
      cdpEnvClient,
      cdpDlClient
    )
}
