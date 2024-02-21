package it.agilelab.provisioning.impala.table.provisioner.context

import cats.implicits.toBifunctorOps
import it.agilelab.provisioning.commons.client.cdp.dl.CdpDlClient
import it.agilelab.provisioning.commons.client.cdp.env.CdpEnvClient
import it.agilelab.provisioning.commons.config.Conf
import it.agilelab.provisioning.commons.config.ConfError.ConfKeyNotFoundErr
import it.agilelab.provisioning.impala.table.provisioner.context.ContextError.{
  ClientError,
  ConfigurationError
}

import scala.util.Try

class ProvisionerContext(
    val deployRoleUser: String,
    val deployRolePwd: String,
    val rangerRoleUser: String,
    val rangerRolePwd: String
)

class CDPPublicProvisionerContext(
    deployRoleUser: String,
    deployRolePwd: String,
    rangerRoleUser: String,
    rangerRolePwd: String,
    val cdpEnvClient: CdpEnvClient,
    val cdpDlClient: CdpDlClient
) extends ProvisionerContext(deployRoleUser, deployRolePwd, rangerRoleUser, rangerRolePwd)

object ProvisionerContext {

  private val CDP_DEPLOY_ROLE_USER = "CDP_DEPLOY_ROLE_USER"
  private val CDP_DEPLOY_ROLE_PASSWORD = "CDP_DEPLOY_ROLE_PASSWORD"

  def initPublic(
      conf: Conf
  ): Either[ContextError, CDPPublicProvisionerContext] =
    for {
      deployRoleUser <- conf.get(CDP_DEPLOY_ROLE_USER).leftMap(e => ConfigurationError(e))
      deployRolePwd  <- conf.get(CDP_DEPLOY_ROLE_PASSWORD).leftMap(e => ConfigurationError(e))
      rangerRoleUser <- Try(
        ApplicationConfiguration.rangerConfig.getString(
          ApplicationConfiguration.RANGER_USERNAME)).toEither.leftMap(_ =>
        ConfigurationError(ConfKeyNotFoundErr(ApplicationConfiguration.RANGER_USERNAME)))
      rangerRolePwd <- Try(
        ApplicationConfiguration.rangerConfig.getString(
          ApplicationConfiguration.RANGER_PASSWORD)).toEither.leftMap(_ =>
        ConfigurationError(ConfKeyNotFoundErr(ApplicationConfiguration.RANGER_PASSWORD)))
      cdpEnvClient <- CdpEnvClient.defaultWithAudit().leftMap(e => ClientError("CdpEnvClient", e))
      cdpDlClient  <- CdpDlClient.defaultWithAudit().leftMap(e => ClientError("CdpDlClient", e))
    } yield new CDPPublicProvisionerContext(
      deployRoleUser,
      deployRolePwd,
      rangerRoleUser,
      rangerRolePwd,
      cdpEnvClient,
      cdpDlClient
    )

  def initPrivate(
      conf: Conf
  ): Either[ContextError, ProvisionerContext] = for {
    deployRoleUser <- conf.get(CDP_DEPLOY_ROLE_USER).leftMap(e => ConfigurationError(e))
    deployRolePwd  <- conf.get(CDP_DEPLOY_ROLE_PASSWORD).leftMap(e => ConfigurationError(e))
    rangerRoleUser <- Try(
      ApplicationConfiguration.rangerConfig.getString(
        ApplicationConfiguration.RANGER_USERNAME)).toEither.leftMap(_ =>
      ConfigurationError(ConfKeyNotFoundErr(ApplicationConfiguration.RANGER_USERNAME)))
    rangerRolePwd <- Try(
      ApplicationConfiguration.rangerConfig.getString(
        ApplicationConfiguration.RANGER_PASSWORD)).toEither.leftMap(_ =>
      ConfigurationError(ConfKeyNotFoundErr(ApplicationConfiguration.RANGER_PASSWORD)))
  } yield new ProvisionerContext(
    deployRoleUser,
    deployRolePwd,
    rangerRoleUser,
    rangerRolePwd
  )
}
