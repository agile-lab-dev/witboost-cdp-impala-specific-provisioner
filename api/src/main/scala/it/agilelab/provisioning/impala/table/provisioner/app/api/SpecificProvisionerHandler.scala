package it.agilelab.provisioning.impala.table.provisioner.app.api

import cats.effect.IO
import io.circe.Json
import io.circe.generic.auto._
import it.agilelab.provisioning.api.generated.definitions._
import it.agilelab.provisioning.api.generated.{ Handler, Resource }
import it.agilelab.provisioning.commons.principalsmapping.CdpIamPrincipals
import it.agilelab.provisioning.impala.table.provisioner.app.api.mapping.{
  ProvisioningStatusMapper,
  ValidationErrorMapper
}
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaCdw
import it.agilelab.provisioning.mesh.self.service.api.controller.ProvisionerController
import it.agilelab.provisioning.mesh.self.service.api.model.{ ApiError, ApiRequest }

class SpecificProvisionerHandler(
    provisioner: ProvisionerController[Json, ImpalaCdw, CdpIamPrincipals]
) extends Handler[IO] {

  private val NotImplementedError = SystemError(
    error = "Endpoint not implemented",
    userMessage = Some("The requested feature hasn't been implemented"),
    input = None,
    inputErrorField = None,
    moreInfo =
      Some(ErrorMoreInfo(problems = Vector("Endpoint not implemented"), solutions = Vector.empty))
  )

  override def provision(respond: Resource.ProvisionResponse.type)(
      body: ProvisioningRequest
  ): IO[Resource.ProvisionResponse] = IO {
    provisioner.provision(ApiRequest.ProvisioningRequest(body.descriptor)) match {
      case Left(error: ApiError.ValidationError) =>
        Resource.ProvisionResponse.BadRequest(RequestValidationError(error.errors.toVector))
      case Left(error: ApiError.SystemError) =>
        Resource.ProvisionResponse.InternalServerError(SystemError(error.error))
      case Right(status) => Resource.ProvisionResponse.Ok(ProvisioningStatusMapper.from(status))
    }
  }

  override def runReverseProvisioning(respond: Resource.RunReverseProvisioningResponse.type)(
      body: ReverseProvisioningRequest
  ): IO[Resource.RunReverseProvisioningResponse] = IO {
    Resource.RunReverseProvisioningResponse.InternalServerError(NotImplementedError)
  }

  override def unprovision(respond: Resource.UnprovisionResponse.type)(
      body: ProvisioningRequest
  ): IO[Resource.UnprovisionResponse] = IO {
    provisioner.unprovision(ApiRequest.ProvisioningRequest(body.descriptor)) match {
      case Left(error: ApiError.ValidationError) =>
        Resource.UnprovisionResponse.BadRequest(RequestValidationError(error.errors.toVector))
      case Left(error: ApiError.SystemError) =>
        Resource.UnprovisionResponse.InternalServerError(SystemError(error.error))
      case Right(status) => Resource.UnprovisionResponse.Ok(ProvisioningStatusMapper.from(status))
    }
  }

  override def updateacl(respond: Resource.UpdateaclResponse.type)(
      body: UpdateAclRequest
  ): IO[Resource.UpdateaclResponse] = IO {
    provisioner.updateAcl(
      ApiRequest.UpdateAclRequest(
        body.refs,
        ApiRequest.ProvisionInfo(body.provisionInfo.request, body.provisionInfo.result))
    ) match {
      case Left(error: ApiError.ValidationError) =>
        Resource.UpdateaclResponse.BadRequest(RequestValidationError(error.errors.toVector))
      case Left(error: ApiError.SystemError) =>
        Resource.UpdateaclResponse.InternalServerError(SystemError(error.error))
      case Right(status) => Resource.UpdateaclResponse.Ok(ProvisioningStatusMapper.from(status))
    }
  }

  override def validate(respond: Resource.ValidateResponse.type)(
      body: ProvisioningRequest
  ): IO[Resource.ValidateResponse] = IO {
    provisioner.validate(ApiRequest.ProvisioningRequest(body.descriptor)) match {
      case Left(error: ApiError.SystemError) =>
        Resource.ValidateResponse.InternalServerError(SystemError(error.error))
      case Right(result) =>
        Resource.ValidateResponse.Ok(
          ValidationResult(result.valid, result.error.map(ValidationErrorMapper.from)))
    }
  }

  override def asyncValidate(respond: Resource.AsyncValidateResponse.type)(
      body: ProvisioningRequest
  ): IO[Resource.AsyncValidateResponse] = IO {
    Resource.AsyncValidateResponse.InternalServerError(NotImplementedError)
  }

  override def getValidationStatus(respond: Resource.GetValidationStatusResponse.type)(
      token: String
  ): IO[Resource.GetValidationStatusResponse] = IO {
    val error = "Asynchronous task provisioning is not yet implemented"
    Resource.GetValidationStatusResponse.BadRequest(
      RequestValidationError(
        errors = Vector(error),
        userMessage = Some(error),
        input = Some(token),
        inputErrorField = None,
        moreInfo = Some(ErrorMoreInfo(problems = Vector(error), Vector.empty))
      ))
  }

  override def getStatus(respond: Resource.GetStatusResponse.type)(
      token: String
  ): IO[Resource.GetStatusResponse] = IO {
    val error = "Asynchronous task provisioning is not yet implemented"
    Resource.GetStatusResponse.BadRequest(
      RequestValidationError(
        errors = Vector(error),
        userMessage = Some(error),
        input = Some(token),
        inputErrorField = None,
        moreInfo = Some(ErrorMoreInfo(problems = Vector(error), Vector.empty))
      ))
  }

  override def getReverseProvisioningStatus(
      respond: Resource.GetReverseProvisioningStatusResponse.type
  )(token: String): IO[Resource.GetReverseProvisioningStatusResponse] = IO {
    Resource.GetReverseProvisioningStatusResponse.InternalServerError(NotImplementedError)
  }
}
