package it.agilelab.provisioning.impala.table.provisioner.app.api

import cats.effect.IO
import io.circe.{ Decoder, Json }
import it.agilelab.provisioning.api.generated.Resource
import it.agilelab.provisioning.api.generated.definitions.{
  DescriptorKind,
  ProvisioningRequest,
  SystemError,
  ValidationError,
  ValidationResult
}
import it.agilelab.provisioning.impala.table.provisioner.app.api.helpers.ProvisionerControllerMock
import it.agilelab.provisioning.impala.table.provisioner.app.api.routes.helpers.HandlerTestBase
import it.agilelab.provisioning.mesh.self.service.api.model._
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{ Method, Request, Response, Status }

class ValidateHandlerTest extends HandlerTestBase {
  "The server" should "return a 200 with no error when the descriptor validation succeeds" in {
    val handler = new SpecificProvisionerHandler(new ProvisionerControllerMock)
    val response: IO[Response[IO]] = new Resource[IO]()
      .routes(handler)
      .orNotFound
      .run(
        Request(method = Method.POST, uri = uri"/v1/validate")
          .withEntity(ProvisioningRequest(DescriptorKind.ComponentDescriptor, "a-yaml-descriptor"))
      )
    val expected = ValidationResult(valid = true)

    check[ValidationResult](response, Status.Ok, Some(expected)) shouldBe true
  }

  it should "return a 200 with a list of errors when the validation fails" in {
    val errors = Vector("first error", "second error")
    val controllerMock = new ProvisionerControllerMock {
      override def validate(request: ApiRequest.ProvisioningRequest)(implicit
          decoderPd: Decoder[ProvisioningDescriptor[Json]],
          decoderCmp: Decoder[Component[Json]]
      ): Either[ApiError.SystemError, ApiResponse.ValidationResult] =
        Right(ApiResponse.invalid(errors: _*))
    }

    val handler = new SpecificProvisionerHandler(controllerMock)
    val response: IO[Response[IO]] = new Resource[IO]()
      .routes(handler)
      .orNotFound
      .run(
        Request(method = Method.POST, uri = uri"/v1/validate")
          .withEntity(
            ProvisioningRequest(DescriptorKind.ComponentDescriptor, "a-wrong-yaml-descriptor"))
      )

    val expected = ValidationResult(valid = false, error = Some(ValidationError(errors)))

    check[ValidationResult](response, Status.Ok, Some(expected)) shouldBe true
  }

  it should "return a 500 with with meaningful error on validate exception" in {
    val error = "first error"
    val controllerMock = new ProvisionerControllerMock {
      override def validate(request: ApiRequest.ProvisioningRequest)(implicit
          decoderPd: Decoder[ProvisioningDescriptor[Json]],
          decoderCmp: Decoder[Component[Json]]
      ): Either[ApiError.SystemError, ApiResponse.ValidationResult] = Left(ApiError.sysErr(error))
    }

    val handler = new SpecificProvisionerHandler(controllerMock)
    val response: IO[Response[IO]] = new Resource[IO]()
      .routes(handler)
      .orNotFound
      .run(
        Request(method = Method.POST, uri = uri"/v1/validate")
          .withEntity(
            ProvisioningRequest(DescriptorKind.ComponentDescriptor, "a-wrong-yaml-descriptor"))
      )
    val expected = SystemError(error)

    check[SystemError](response, Status.InternalServerError, Some(expected)) shouldBe true
  }
}
