package it.agilelab.provisioning.impala.table.provisioner.app.config

import cats.implicits.toBifunctorOps
import io.circe.Json
import it.agilelab.provisioning.commons.config.Conf
import it.agilelab.provisioning.commons.config.ConfError.{ ConfDecodeErr, ConfKeyNotFoundErr }
import it.agilelab.provisioning.commons.principalsmapping.{ CdpIamPrincipals, PrincipalsMapper }
import it.agilelab.provisioning.commons.validator.Validator
import it.agilelab.provisioning.impala.table.provisioner.app.api.validator.ImpalaCdwValidator
import it.agilelab.provisioning.impala.table.provisioner.app.api.validator.ImpalaCdwValidator.impalaCdwValidator
import it.agilelab.provisioning.impala.table.provisioner.clients.cdp.{
  CDPPublicHostProvider,
  ConfigHostProvider
}
import it.agilelab.provisioning.impala.table.provisioner.context.CloudType.CloudType
import it.agilelab.provisioning.impala.table.provisioner.context.ContextError.ConfigurationError
import it.agilelab.provisioning.impala.table.provisioner.context._
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaProvisionerResource
import it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.provider.RangerGatewayProvider
import it.agilelab.provisioning.impala.table.provisioner.gateway.resource.acl.ImpalaAccessControlGateway
import it.agilelab.provisioning.impala.table.provisioner.gateway.resource.{
  CDPPrivateImpalaOutputPortGateway,
  CDPPrivateImpalaStorageAreaGateway,
  ImpalaGateway,
  ImpalaTableOutputPortGateway
}
import it.agilelab.provisioning.impala.table.provisioner.gateway.table.ExternalTableGateway
import it.agilelab.provisioning.impala.table.provisioner.gateway.view.ViewGateway
import it.agilelab.provisioning.mesh.self.service.api.controller.ProvisionerController
import it.agilelab.provisioning.mesh.self.service.api.model.ProvisionRequest
import it.agilelab.provisioning.mesh.self.service.core.gateway.{
  ComponentGateway,
  ComponentGatewayError,
  PermissionlessComponentGateway
}
import it.agilelab.provisioning.mesh.self.service.core.model.ProvisionCommand
import it.agilelab.provisioning.mesh.self.service.core.provisioner.Provisioner

import scala.util.{ Failure, Success, Try }

object ImpalaProvisionerController {
  def apply(conf: Conf): Either[ContextError, ProvisionerController[Json, Json, CdpIamPrincipals]] =
    for {
      cloudType <- Try(
        CloudType.withName(ApplicationConfiguration.provisionerConfig.getString(
          ApplicationConfiguration.PROVISION_CLOUD_TYPE))).toEither
        .leftMap(_ =>
          ConfigurationError(ConfKeyNotFoundErr(ApplicationConfiguration.PROVISION_CLOUD_TYPE)))
      validator <- createValidator(cloudType, conf)
      principalsMapper <- new PrincipalsMapperPluginLoader().load(
        ApplicationConfiguration.principalsMapperConfig)
      impalaGateway <- cloudType match {
        case CloudType.Public  => createPublicGateway(conf, principalsMapper)
        case CloudType.Private => createPrivateGateway(conf, principalsMapper)
      }

    } yield ProvisionerController.defaultAclWithAudit[Json, Json, CdpIamPrincipals](
      validator,
      // Currently supporting only synchronous operations
      Provisioner.defaultSync[Json, Json, Json, CdpIamPrincipals](impalaGateway),
      // TODO we should create our custom controller to avoid to inject a state repo
      new MemoryStateRepository,
      principalsMapper
    )

  private def createPublicGateway(
      conf: Conf,
      principalsMapper: PrincipalsMapper[CdpIamPrincipals]
  ): Either[ContextError, ComponentGateway[Json, Json, Json, CdpIamPrincipals]] = for {
    ctx <- ProvisionerContext
      .initPublic(conf)
    tableGateway <- getExternalTableGateway(ctx)
    hostProvider = new CDPPublicHostProvider(ctx.cdpEnvClient, ctx.cdpDlClient)
    rangerGatewayProvider = new RangerGatewayProvider(ctx.deployRoleUser, ctx.deployRolePwd)
    aclGateway = new ImpalaAccessControlGateway(
      serviceRole = ctx.deployRoleUser,
      rangerGatewayProvider = rangerGatewayProvider,
      principalsMapper = principalsMapper
    )
  } yield new ImpalaGateway(
    outputPortGateway = new ImpalaTableOutputPortGateway(
      ctx.deployRoleUser,
      hostProvider,
      tableGateway,
      rangerGatewayProvider,
      aclGateway
    ),
    storageAreaGateway = new PermissionlessComponentGateway[Json, Json, ImpalaProvisionerResource] {
      val error: Either[ComponentGatewayError, ImpalaProvisionerResource] = Left(
        ComponentGatewayError(
          "The provisioner currently doesn't support storage areas on CDP Public Cloud"))
      override def create(
          provisionCommand: ProvisionCommand[Json, Json]
      ): Either[ComponentGatewayError, ImpalaProvisionerResource] = error
      override def destroy(
          provisionCommand: ProvisionCommand[Json, Json]
      ): Either[ComponentGatewayError, ImpalaProvisionerResource] = error
    }
  )

  private def createPrivateGateway(
      conf: Conf,
      principalsMapper: PrincipalsMapper[CdpIamPrincipals]
  ): Either[ContextError, ComponentGateway[Json, Json, Json, CdpIamPrincipals]] = for {
    ctx          <- ProvisionerContext.initPrivate(conf)
    tableGateway <- getExternalTableGateway(ctx)
    viewGateway  <- getViewGateway(ctx)
    rangerGatewayProvider = new RangerGatewayProvider(ctx.rangerRoleUser, ctx.rangerRolePwd)
    aclGateway = new ImpalaAccessControlGateway(
      serviceRole = ctx.deployRoleUser,
      rangerGatewayProvider = rangerGatewayProvider,
      principalsMapper = principalsMapper
    )
    hostProvider = new ConfigHostProvider()
  } yield new ImpalaGateway(
    outputPortGateway = new CDPPrivateImpalaOutputPortGateway(
      ctx.deployRoleUser,
      hostProvider,
      tableGateway,
      viewGateway,
      rangerGatewayProvider,
      aclGateway
    ),
    storageAreaGateway = new CDPPrivateImpalaStorageAreaGateway(
      ctx.deployRoleUser,
      hostProvider,
      tableGateway,
      viewGateway,
      rangerGatewayProvider,
      aclGateway
    ))

  private def getExternalTableGateway(
      ctx: ProvisionerContext
  ): Either[ContextError, ExternalTableGateway] =
    Try {
      ApplicationConfiguration.impalaConfig
        .getConfig(ApplicationConfiguration.JDBC_CONFIG)
        .getString(ApplicationConfiguration.JDBC_AUTH_TYPE)
    } match {
      case Success(ApplicationConfiguration.JDBC_SIMPLE_AUTH) =>
        Right(ExternalTableGateway.impalaWithAudit(ctx.deployRoleUser, ctx.deployRolePwd))
      case Success(ApplicationConfiguration.JDBC_KERBEROS_AUTH) =>
        Right(ExternalTableGateway.kerberizedImpalaWithAudit())
      case Success(value) =>
        Left(
          ConfigurationError(
            ConfDecodeErr(s"JDBC Authentication type not supported. Received '$value'")))
      case Failure(_) =>
        Left(
          ConfigurationError(ConfKeyNotFoundErr(
            s"${ApplicationConfiguration.JDBC_CONFIG}.${ApplicationConfiguration.JDBC_AUTH_TYPE}")))
    }

  private def getViewGateway(
      ctx: ProvisionerContext
  ): Either[ContextError, ViewGateway] =
    Try {
      ApplicationConfiguration.impalaConfig
        .getConfig(ApplicationConfiguration.JDBC_CONFIG)
        .getString(ApplicationConfiguration.JDBC_AUTH_TYPE)
    } match {
      case Success(ApplicationConfiguration.JDBC_SIMPLE_AUTH) =>
        Right(ViewGateway.impalaWithAudit(ctx.deployRoleUser, ctx.deployRolePwd))
      case Success(ApplicationConfiguration.JDBC_KERBEROS_AUTH) =>
        Right(ViewGateway.kerberizedImpalaWithAudit())
      case Success(value) =>
        Left(
          ConfigurationError(
            ConfDecodeErr(s"JDBC Authentication type not supported. Received '$value'")))
      case Failure(_) =>
        Left(
          ConfigurationError(ConfKeyNotFoundErr(
            s"${ApplicationConfiguration.JDBC_CONFIG}.${ApplicationConfiguration.JDBC_AUTH_TYPE}")))
    }

  private def createValidator(
      cloudType: CloudType,
      conf: Conf
  ): Either[ContextError, Validator[ProvisionRequest[Json, Json]]] =
    cloudType match {
      case CloudType.Public =>
        ValidatorContext.initPublic(conf).map { validatorCtx =>
          impalaCdwValidator(
            cdpValidator = validatorCtx.cdpValidator,
            locationValidator = validatorCtx.locationValidator
          )
        }
      case CloudType.Private =>
        ValidatorContext.initPrivate(conf).map { validatorCtx =>
          ImpalaCdwValidator.privateImpalaCdwValidator(validatorCtx.locationValidator)
        }
    }

}
