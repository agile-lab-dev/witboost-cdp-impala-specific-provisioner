package it.agilelab.provisioning.impala.table.provisioner.app.config

import cats.implicits.toBifunctorOps
import io.circe.Json
import io.circe.generic.auto._
import it.agilelab.provisioning.commons.config.Conf
import it.agilelab.provisioning.commons.config.ConfError.ConfKeyNotFoundErr
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

import scala.util.Try

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
  ): Either[ContextError, ProvisionerController[Json, Json, CdpIamPrincipals]] = for {
    impalaValidator <- ValidatorContext
      .initPublic(conf)
      .map { ctx =>
        impalaCdwValidator(
          cdpValidator = ctx.cdpValidator,
          locationValidator = ctx.locationValidator
        )
      }
    controller <- ProvisionerContext
      .initPublic(conf)
      .map { ctx =>
        val hostProvider = new CDPPublicHostProvider(ctx.cdpEnvClient, ctx.cdpDlClient)
        val rangerGatewayProvider = new RangerGatewayProvider(ctx.deployRoleUser, ctx.deployRolePwd)
        ProvisionerController.defaultAclWithAudit[Json, Json, CdpIamPrincipals](
          impalaValidator,
          // Currently supporting only synchronous operations
          Provisioner.defaultSync[Json, Json, ImpalaTableOutputPortResource, CdpIamPrincipals](
            new ImpalaTableOutputPortGateway(
              ctx.deployRoleUser,
              hostProvider,
              ExternalTableGateway.impalaWithAudit(ctx.deployRoleUser, ctx.deployRolePwd),
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
  } yield controller

  private def createPrivateProvisionerController(
      conf: Conf
  ): Either[ContextError, ProvisionerController[Json, Json, CdpIamPrincipals]] = for {
    impalaValidator <- ValidatorContext.initPrivate(conf).map { ctx =>
      ImpalaCdwValidator.privateImpalaCdwValidator(ctx.locationValidator)
    }
    controller <- ProvisionerContext
      .initPrivate(conf)
      .map { ctx =>
        val rangerGatewayProvider = new RangerGatewayProvider(ctx.rangerRoleUser, ctx.rangerRolePwd)
        ProvisionerController.defaultAclWithAudit[Json, Json, CdpIamPrincipals](
          impalaValidator,
          // Currently supporting only synchronous operations
          Provisioner.defaultSync[Json, Json, ImpalaTableOutputPortResource, CdpIamPrincipals](
            new CDPPrivateImpalaTableOutputPortGateway(
              ctx.deployRoleUser,
              new ConfigHostProvider(),
              ExternalTableGateway.impalaWithAudit(ctx.deployRoleUser, ctx.deployRolePwd),
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
  } yield controller

}
