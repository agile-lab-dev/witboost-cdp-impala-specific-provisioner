package it.agilelab.provisioning.impala.table.provisioner.app.config

import io.circe.Json
import io.circe.generic.auto._
import it.agilelab.provisioning.commons.config.Conf
import it.agilelab.provisioning.impala.table.provisioner.app.api.validator.ImpalaCdwValidator.impalaCdwValidator
import it.agilelab.provisioning.impala.table.provisioner.clients.cdp.HostProvider
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
import it.agilelab.provisioning.impala.table.provisioner.gateway.resource.ImpalaTableOutputPortGateway
import it.agilelab.provisioning.impala.table.provisioner.gateway.table.ExternalTableGateway
import it.agilelab.provisioning.mesh.self.service.api.controller.ProvisionerController
import it.agilelab.provisioning.mesh.self.service.core.provisioner.Provisioner

object ImpalaProvisionerController {
  def apply(
      conf: Conf
  ): Either[ContextError, ProvisionerController[Json, ImpalaCdw]] = for {
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
        ProvisionerController.default[Json, ImpalaCdw](
          impalaValidator,
          // Currently supporting only synchronous operations
          Provisioner.defaultSync[Json, ImpalaCdw, ImpalaTableOutputPortResource](
            new ImpalaTableOutputPortGateway(
              ctx.deployRoleUser,
              new HostProvider(ctx.cdpEnvClient, ctx.cdpDlClient),
              ExternalTableGateway.impala(ctx.deployRoleUser, ctx.deployRolePwd),
              new RangerGatewayProvider(ctx.deployRoleUser, ctx.deployRolePwd)
            )
          ),
          // TODO we should create our custom controller to avoid to inject a state repo
          new MemoryStateRepository
        )
      }
  } yield controller

}
