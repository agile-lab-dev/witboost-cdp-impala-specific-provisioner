package it.agilelab.provisioning.impala.table.provisioner.gateway.resource

import io.circe.Json
import it.agilelab.provisioning.commons.principalsmapping.CdpIamPrincipals
import it.agilelab.provisioning.impala.table.provisioner.core.model.ComponentDecodeError.DecodeErr
import it.agilelab.provisioning.mesh.self.service.api.model.Component.{
  OutputPort,
  StorageArea,
  Workload
}
import it.agilelab.provisioning.mesh.self.service.core.gateway.{
  ComponentGateway,
  ComponentGatewayError
}
import it.agilelab.provisioning.mesh.self.service.core.model.ProvisionCommand
import cats.implicits.showInterpolator
import io.circe.generic.auto.exportEncoder
import io.circe.syntax.EncoderOps
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaTableResource

class ImpalaGateway(
    outputPortGateway: ComponentGateway[Json, Json, ImpalaTableResource, CdpIamPrincipals],
    storageAreaGateway: ComponentGateway[Json, Json, ImpalaTableResource, CdpIamPrincipals]
) extends ComponentGateway[Json, Json, Json, CdpIamPrincipals] {

  override def create(
      provisionCommand: ProvisionCommand[Json, Json]
  ): Either[ComponentGatewayError, Json] = provisionCommand.provisionRequest.component match {
    case Some(_: OutputPort[Json]) =>
      outputPortGateway.create(provisionCommand).map(_.asJson)
    case Some(_: StorageArea[Json]) =>
      storageAreaGateway.create(provisionCommand).map(_.asJson)
    case Some(_: Workload[_]) =>
      Left(
        ComponentGatewayError(
          "Received kind 'workload' which is not supported on this provisioner"))
    case None =>
      Left(
        ComponentGatewayError(
          show"${DecodeErr("Received provisioning request does not contain a component")}"
        )
      )
  }

  override def destroy(
      provisionCommand: ProvisionCommand[Json, Json]
  ): Either[ComponentGatewayError, Json] = provisionCommand.provisionRequest.component match {
    case Some(_: OutputPort[Json]) =>
      outputPortGateway.destroy(provisionCommand).map(_.asJson)
    case Some(_: StorageArea[Json]) =>
      storageAreaGateway.destroy(provisionCommand).map(_.asJson)
    case Some(_: Workload[_]) =>
      Left(
        ComponentGatewayError(
          "Received kind 'workload' which is not supported on this provisioner"))
    case None =>
      Left(
        ComponentGatewayError(
          show"${DecodeErr("Received provisioning request does not contain a component")}"
        )
      )
  }

  override def updateAcl(
      provisionCommand: ProvisionCommand[Json, Json],
      refs: Set[CdpIamPrincipals]
  ): Either[ComponentGatewayError, Set[CdpIamPrincipals]] =
    provisionCommand.provisionRequest.component match {
      case Some(_: OutputPort[Json]) =>
        outputPortGateway.updateAcl(provisionCommand, refs)
      case Some(_: StorageArea[Json]) =>
        Left(
          ComponentGatewayError("storage components don't support update ACL tasks")
        )
      case Some(_: Workload[Json]) =>
        Left(
          ComponentGatewayError("workload components don't support update ACL tasks")
        )
      case None =>
        Left(
          ComponentGatewayError(
            show"${DecodeErr("Received provisioning request does not contain a component")}"
          )
        )
    }
}
