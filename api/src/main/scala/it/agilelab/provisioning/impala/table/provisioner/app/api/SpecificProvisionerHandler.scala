package it.agilelab.provisioning.impala.table.provisioner.app.api

import cats.effect.IO
import cats.implicits.toShow
import com.typesafe.scalalogging.StrictLogging
import io.circe.Json
import io.circe.generic.auto._
import it.agilelab.provisioning.api.generated.definitions._
import it.agilelab.provisioning.api.generated.{ Handler, Resource }
import it.agilelab.provisioning.commons.principalsmapping.CdpIamPrincipals
import it.agilelab.provisioning.impala.table.provisioner.app.api.mapping.{
  ProvisioningStatusMapper,
  ValidationErrorMapper
}
import it.agilelab.provisioning.mesh.self.service.api.controller.ProvisionerController
import it.agilelab.provisioning.mesh.self.service.api.model.{ ApiError, ApiRequest, ApiResponse }
import retry.{ retryingOnFailures, RetryPolicies, RetryPolicy }

class SpecificProvisionerHandler(
    provisioner: ProvisionerController[Json, Json, CdpIamPrincipals],
    retryPolicy: RetryPolicy[IO] = RetryPolicies.alwaysGiveUp
) extends Handler[IO]
    with StrictLogging {

  private val NotImplementedError = SystemError(
    error = "Endpoint not implemented",
    userMessage = Some("The requested feature hasn't been implemented"),
    input = None,
    inputErrorField = None,
    moreInfo =
      Some(ErrorMoreInfo(problems = Vector("Endpoint not implemented"), solutions = Vector.empty))
  )

  /** Function to discriminate between errors worth retrying the whole request (based on [[retryPolicy]]) or not
    * @param maybeError Either a good result, or a System or Validation Error. Only System Errors are worth retrying
    * @return IO of Boolean whether to keep the result or not
    */
  private def isNonRetryableError[T](
      maybeError: Either[ApiError, T]
  ): IO[Boolean] = maybeError match {
    case Left(_: ApiError.SystemError) => IO.pure(false)
    case _                             => IO.pure(true)
  }

  override def provision(respond: Resource.ProvisionResponse.type)(
      body: ProvisioningRequest
  ): IO[Resource.ProvisionResponse] =
    retryingOnFailures[Either[ApiError, ApiResponse.ProvisioningStatus]](
      policy = retryPolicy,
      wasSuccessful = isNonRetryableError[ApiResponse.ProvisioningStatus],
      onFailure = (outcome, retryInfo) =>
        IO(
          logger.error(
            "Unprovision request failed. Retry number {} - Will retry again? {} - Details {}",
            retryInfo.retriesSoFar,
            retryInfo.upcomingDelay.fold("No")(upcoming =>
              s"Yes. Retrying in ${upcoming.toMillis}ms"),
            outcome.fold(_.show, _.status)
          ))
    )(IO {
      provisioner.provision(ApiRequest.ProvisioningRequest(body.descriptor))
    }).map {
      case Left(error: ApiError.ValidationError) =>
        Resource.ProvisionResponse.BadRequest(RequestValidationError(error.errors.toVector))
      case Left(error: ApiError.SystemError) =>
        Resource.ProvisionResponse.InternalServerError(SystemError(error.error))
      case Right(status) => Resource.ProvisionResponse.Ok(ProvisioningStatusMapper.from(status))
    }

  override def runReverseProvisioning(respond: Resource.RunReverseProvisioningResponse.type)(
      body: ReverseProvisioningRequest
  ): IO[Resource.RunReverseProvisioningResponse] = IO {
    Resource.RunReverseProvisioningResponse.InternalServerError(NotImplementedError)
  }

  override def unprovision(respond: Resource.UnprovisionResponse.type)(
      body: ProvisioningRequest
  ): IO[Resource.UnprovisionResponse] =
    retryingOnFailures[Either[ApiError, ApiResponse.ProvisioningStatus]](
      policy = retryPolicy,
      wasSuccessful = isNonRetryableError[ApiResponse.ProvisioningStatus],
      onFailure = (outcome, retryInfo) =>
        IO(
          logger.error(
            "Unprovision request failed. Retry number {} - Will retry again? {} - Details {}",
            retryInfo.retriesSoFar,
            retryInfo.upcomingDelay.fold("No")(upcoming =>
              s"Yes. Retrying in ${upcoming.toMillis}ms"),
            outcome.fold(_.show, _.status)
          ))
    )(IO {
      provisioner.unprovision(ApiRequest.ProvisioningRequest(body.descriptor))
    }).map {
      case Left(error: ApiError.ValidationError) =>
        Resource.UnprovisionResponse.BadRequest(RequestValidationError(error.errors.toVector))
      case Left(error: ApiError.SystemError) =>
        Resource.UnprovisionResponse.InternalServerError(SystemError(error.error))
      case Right(status) => Resource.UnprovisionResponse.Ok(ProvisioningStatusMapper.from(status))
    }

  override def updateacl(respond: Resource.UpdateaclResponse.type)(
      body: UpdateAclRequest
  ): IO[Resource.UpdateaclResponse] =
    retryingOnFailures[Either[ApiError, ApiResponse.ProvisioningStatus]](
      policy = retryPolicy,
      wasSuccessful = isNonRetryableError[ApiResponse.ProvisioningStatus],
      onFailure = (outcome, retryInfo) =>
        IO(
          logger.error(
            "Update ACL request failed. Retry number {} - Will retry again? {} - Details {}",
            retryInfo.retriesSoFar,
            retryInfo.upcomingDelay.fold("No")(upcoming =>
              s"Yes. Retrying in ${upcoming.toMillis}ms"),
            outcome.fold(_.show, _.status)
          ))
    )(IO {
      provisioner.updateAcl(
        ApiRequest.UpdateAclRequest(
          body.refs,
          ApiRequest.ProvisionInfo(body.provisionInfo.request, body.provisionInfo.result))
      )
    }).map {
      case Left(error: ApiError.ValidationError) =>
        Resource.UpdateaclResponse.BadRequest(RequestValidationError(error.errors.toVector))
      case Left(error: ApiError.SystemError) =>
        Resource.UpdateaclResponse.InternalServerError(SystemError(error.error))
      case Right(status) => Resource.UpdateaclResponse.Ok(ProvisioningStatusMapper.from(status))
    }

  override def validate(respond: Resource.ValidateResponse.type)(
      body: ProvisioningRequest
  ): IO[Resource.ValidateResponse] =
    retryingOnFailures[Either[ApiError.SystemError, ApiResponse.ValidationResult]](
      policy = retryPolicy,
      wasSuccessful = isNonRetryableError[ApiResponse.ValidationResult],
      onFailure = (outcome, retryInfo) =>
        IO(
          logger.error(
            "Validate request failed. Retry number {} - Will retry again? {} - Details {}",
            retryInfo.retriesSoFar,
            retryInfo.upcomingDelay.fold("No")(upcoming =>
              s"Yes. Retrying in ${upcoming.toMillis}ms"),
            outcome.fold(_.error, _.valid)
          ))
    )(IO {
      provisioner.validate(ApiRequest.ProvisioningRequest(body.descriptor))
    }).map {
      case Left(error: ApiError.SystemError) =>
        Resource.ValidateResponse.InternalServerError(SystemError(error.error))
      case Right(result) =>
        Resource.ValidateResponse.Ok(
          ValidationResult(result.valid, result.error.map(ValidationErrorMapper.from)))
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
