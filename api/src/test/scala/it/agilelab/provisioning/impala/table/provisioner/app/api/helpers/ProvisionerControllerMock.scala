package it.agilelab.provisioning.impala.table.provisioner.app.api.helpers

import io.circe.{ Decoder, Json }
import it.agilelab.provisioning.commons.principalsmapping.CdpIamPrincipals
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaCdw
import it.agilelab.provisioning.mesh.self.service.api.controller.ProvisionerController
import it.agilelab.provisioning.mesh.self.service.api.model.{
  ApiError,
  ApiRequest,
  ApiResponse,
  Component,
  ProvisioningDescriptor
}

class ProvisionerControllerMock extends ProvisionerController[Json, ImpalaCdw, CdpIamPrincipals] {
  override def validate(request: ApiRequest.ProvisioningRequest)(implicit
      decoderPd: Decoder[ProvisioningDescriptor[Json]],
      decoderCmp: Decoder[Component[ImpalaCdw]]
  ): Either[ApiError.SystemError, ApiResponse.ValidationResult] = Right(ApiResponse.valid())

  override def provision(request: ApiRequest.ProvisioningRequest)(implicit
      decoderPd: Decoder[ProvisioningDescriptor[Json]],
      decoderCmp: Decoder[Component[ImpalaCdw]]
  ): Either[ApiError, ApiResponse.ProvisioningStatus] = Right(ApiResponse.completed("fakeid", None))

  override def getProvisionStatus(id: String): Either[ApiError, ApiResponse.ProvisioningStatus] =
    Right(ApiResponse.completed(id, None))

  override def unprovision(request: ApiRequest.ProvisioningRequest)(implicit
      decoderPd: Decoder[ProvisioningDescriptor[Json]],
      decoderCmp: Decoder[Component[ImpalaCdw]]
  ): Either[ApiError, ApiResponse.ProvisioningStatus] = Right(ApiResponse.completed("fakeid", None))

  override def updateAcl(updateAclRequest: ApiRequest.UpdateAclRequest)(implicit
      decoderPd: Decoder[ProvisioningDescriptor[Json]],
      decoderCmp: Decoder[Component[ImpalaCdw]]
  ): Either[ApiError, ApiResponse.ProvisioningStatus] = Right(ApiResponse.completed("fakeid", None))
}
