package it.agilelab.provisioning.impala.table.provisioner.app.config

import cats.implicits.toBifunctorOps
import io.circe.Json
import io.circe.generic.auto._
import it.agilelab.provisioning.commons.config.Conf
import it.agilelab.provisioning.commons.config.ConfError.{ ConfDecodeErr, ConfKeyNotFoundErr }
import it.agilelab.provisioning.commons.principalsmapping.CdpIamPrincipals
import it.agilelab.provisioning.impala.table.provisioner.app.api.validator.ImpalaCdwValidator
import it.agilelab.provisioning.impala.table.provisioner.app.api.validator.ImpalaCdwValidator.impalaCdwValidator
import it.agilelab.provisioning.impala.table.provisioner.clients.cdp.{
  CDPPublicHostProvider,
  ConfigHostProvider
}
import it.agilelab.provisioning.impala.table.provisioner.context.CloudType.CloudType
import it.agilelab.provisioning.impala.table.provisioner.context.ContextError.ConfigurationError
import it.agilelab.provisioning.impala.table.provisioner.context.{
  ApplicationConfiguration,
  ContextError,
  MemoryStateRepository,
  ProvisionerContext
}
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaTableOutputPortResource
import it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.provider.RangerGatewayProvider
import it.agilelab.provisioning.impala.table.provisioner.gateway.resource.{
  CDPPrivateImpalaTableOutputPortGateway,
  ImpalaOutputPortAccessControlGateway,
  ImpalaTableOutputPortGateway
}
import it.agilelab.provisioning.impala.table.provisioner.gateway.table.ExternalTableGateway
import it.agilelab.provisioning.mesh.self.service.api.controller.ProvisionerController
import it.agilelab.provisioning.mesh.self.service.core.provisioner.Provisioner

import scala.util.{ Failure, Success, Try }

object ImpalaProvisionerController {
  def apply(
      conf: Conf
  ): Either[ContextError, ProvisionerController[Json, Json, CdpIamPrincipals]] = for {
    cloudType <- Try {
      CloudType.withName(
        ApplicationConfiguration.provisionerConfig
          .getString(ApplicationConfiguration.PROVISION_CLOUD_TYPE)
      )
    }.toEither
      .leftMap(e =>
        ConfigurationError(ConfKeyNotFoundErr(ApplicationConfiguration.PROVISION_CLOUD_TYPE)))

    controller <- cloudType match {
      case CloudType.Public  => createPublicProvisionerController(conf)
      case CloudType.Private => createPrivateProvisionerController(conf)
    }
  } yield controller

  def createPublicProvisionerController(
      conf: Conf
  ): Either[ContextError, ProvisionerController[Json, Json, CdpIamPrincipals]] =
    ProvisionerContext
      .initPublic(conf)
      .flatMap { ctx =>
        for {
          impalaValidator <- ValidatorContext.initPublic(conf).map { validatorCtx =>
            impalaCdwValidator(
              cdpValidator = validatorCtx.cdpValidator,
              locationValidator = validatorCtx.locationValidator
            )
          }
          tableGateway <- getExternalTableGateway(ctx)
          hostProvider = new CDPPublicHostProvider(ctx.cdpEnvClient, ctx.cdpDlClient)
          rangerGatewayProvider = new RangerGatewayProvider(ctx.deployRoleUser, ctx.deployRolePwd)
        } yield ProvisionerController.defaultAclWithAudit[Json, Json, CdpIamPrincipals](
          impalaValidator,
          // Currently supporting only synchronous operations
          Provisioner.defaultSync[Json, Json, ImpalaTableOutputPortResource, CdpIamPrincipals](
            new ImpalaTableOutputPortGateway(
              ctx.deployRoleUser,
              hostProvider,
              tableGateway,
              rangerGatewayProvider,
              new ImpalaOutputPortAccessControlGateway(
                serviceRole = ctx.deployRoleUser,
                rangerGatewayProvider = rangerGatewayProvider,
                principalsMapper = ctx.principalsMapper
              )
            )
          ),
          // TODO we should create our custom controller to avoid to inject a state repo
          new MemoryStateRepository,
          ctx.principalsMapper
        )
      }

  private def createPrivateProvisionerController(
      conf: Conf
  ): Either[ContextError, ProvisionerController[Json, Json, CdpIamPrincipals]] =
    ProvisionerContext
      .initPrivate(conf)
      .flatMap { ctx =>
        for {
          impalaValidator <- ValidatorContext.initPrivate(conf).map { validatorCtx =>
            ImpalaCdwValidator.privateImpalaCdwValidator(validatorCtx.locationValidator)
          }
          tableGateway <- getExternalTableGateway(ctx)
          rangerGatewayProvider = new RangerGatewayProvider(ctx.rangerRoleUser, ctx.rangerRolePwd)
        } yield ProvisionerController.defaultAclWithAudit[Json, Json, CdpIamPrincipals](
          impalaValidator,
          // Currently supporting only synchronous operations
          Provisioner.defaultSync[Json, Json, ImpalaTableOutputPortResource, CdpIamPrincipals](
            new CDPPrivateImpalaTableOutputPortGateway(
              ctx.deployRoleUser,
              new ConfigHostProvider(),
              tableGateway,
              rangerGatewayProvider,
              new ImpalaOutputPortAccessControlGateway(
                serviceRole = ctx.deployRoleUser,
                rangerGatewayProvider = rangerGatewayProvider,
                principalsMapper = ctx.principalsMapper
              )
            )
          ),
          // TODO we should create our custom controller to avoid to inject a state repo
          new MemoryStateRepository,
          ctx.principalsMapper
        )
      }

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
}
