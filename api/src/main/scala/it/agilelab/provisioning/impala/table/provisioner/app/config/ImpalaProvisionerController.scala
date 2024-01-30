package it.agilelab.provisioning.impala.table.provisioner.app.config

import io.circe.Json
import io.circe.generic.auto._
import it.agilelab.provisioning.commons.config.Conf
import it.agilelab.provisioning.commons.principalsmapping.CdpIamPrincipals
import it.agilelab.provisioning.impala.table.provisioner.app.api.validator.ImpalaCdwValidator.impalaCdwValidator
import it.agilelab.provisioning.impala.table.provisioner.clients.cdp.CDPPublicHostProvider
import it.agilelab.provisioning.impala.table.provisioner.context.{
  ContextError,
  MemoryStateRepository,
  ProvisionerContext
}
import it.agilelab.provisioning.impala.table.provisioner.core.model.{
  ImpalaCdw,
  ImpalaTableOutputPortResource
}
import it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.provider.RangerGatewayProvider
import it.agilelab.provisioning.impala.table.provisioner.gateway.resource.{
  ImpalaOutputPortAccessControlGateway,
  ImpalaTableOutputPortGateway
}
import it.agilelab.provisioning.impala.table.provisioner.gateway.table.ExternalTableGateway
import it.agilelab.provisioning.mesh.self.service.api.controller.ProvisionerController
import it.agilelab.provisioning.mesh.self.service.core.provisioner.Provisioner

object ImpalaProvisionerController {
  def apply(
      conf: Conf
  ): Either[ContextError, ProvisionerController[Json, Json, CdpIamPrincipals]] = for {
    impalaValidator <- ValidatorContext
      .init(conf)
      .map { ctx =>
        impalaCdwValidator(
          cdpValidator = ctx.cdpValidator,
          locationValidator = ctx.locationValidator
        )
      }
    controller <- ProvisionerContext
      .init(conf)
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

}
