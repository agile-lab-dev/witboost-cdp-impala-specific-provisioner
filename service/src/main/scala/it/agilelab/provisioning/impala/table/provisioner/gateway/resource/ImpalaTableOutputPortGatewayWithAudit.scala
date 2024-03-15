package it.agilelab.provisioning.impala.table.provisioner.gateway.resource

import cats.implicits._
import it.agilelab.provisioning.commons.audit.Audit
import it.agilelab.provisioning.mesh.self.service.core.gateway.{
  ComponentGateway,
  ComponentGatewayError
}
import it.agilelab.provisioning.mesh.self.service.core.model.ProvisionCommand
import io.circe.Json
import it.agilelab.provisioning.commons.principalsmapping.CdpIamPrincipals
import it.agilelab.provisioning.impala.table.provisioner.core.model.{
  ImpalaCdw,
  ImpalaProvisionerResource
}

class ImpalaTableOutputPortGatewayWithAudit(
    componentGateway: ComponentGateway[
      Json,
      ImpalaCdw,
      ImpalaProvisionerResource,
      CdpIamPrincipals
    ],
    audit: Audit
) extends ComponentGateway[Json, ImpalaCdw, ImpalaProvisionerResource, CdpIamPrincipals] {
  private val INFO_MSG = "Executing %s"

  override def create(
      command: ProvisionCommand[Json, ImpalaCdw]
  ): Either[ComponentGatewayError, ImpalaProvisionerResource] = {
    val action = s"Create($command)"
    audit.info(INFO_MSG.format(action))
    val result = componentGateway.create(command)
    auditWithinResult(result, action)
    result
  }

  override def destroy(
      command: ProvisionCommand[Json, ImpalaCdw]
  ): Either[ComponentGatewayError, ImpalaProvisionerResource] = {
    val action = s"Destroy($command)"
    audit.info(INFO_MSG.format(action))
    val result = componentGateway.destroy(command)
    auditWithinResult(result, action)
    result
  }

  private def auditWithinResult[D](
      result: Either[ComponentGatewayError, D],
      action: String
  ): Unit =
    result match {
      case Right(_) => audit.info(show"$action completed successfully")
      case Left(l)  => audit.error(s"$action failed. Details: ${l.error}")
    }

  override def updateAcl(
      provisionCommand: ProvisionCommand[Json, ImpalaCdw],
      refs: Set[CdpIamPrincipals]
  ): Either[ComponentGatewayError, Set[CdpIamPrincipals]] = {
    val action = s"UpdateAcl($provisionCommand)"
    audit.info(INFO_MSG.format(action))
    val result = componentGateway.updateAcl(provisionCommand, refs)
    auditWithinResult(result, action)
    result
  }
}
