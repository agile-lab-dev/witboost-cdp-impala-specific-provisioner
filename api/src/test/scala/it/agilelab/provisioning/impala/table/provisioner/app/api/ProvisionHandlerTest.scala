package it.agilelab.provisioning.impala.table.provisioner.app.api

import cats.effect.IO
import io.circe.generic.auto._
import io.circe.syntax._
import io.circe.{ parser, Decoder, Json }
import it.agilelab.provisioning.api.generated.Resource
import it.agilelab.provisioning.api.generated.definitions._
import it.agilelab.provisioning.commons.support.ParserSupport
import it.agilelab.provisioning.impala.table.provisioner.app.api.helpers.ProvisionerControllerMock
import it.agilelab.provisioning.impala.table.provisioner.app.api.routes.helpers.HandlerTestBase
import it.agilelab.provisioning.impala.table.provisioner.core.model._
import it.agilelab.provisioning.mesh.self.service.api.model._
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{ Method, Request, Response, Status }

class ProvisionHandlerTest extends HandlerTestBase with ParserSupport {
  "The server" should "return a 200 with COMPLETED on a successful provision" in {
    val handler = new SpecificProvisionerHandler(new ProvisionerControllerMock)
    val response: IO[Response[IO]] = new Resource[IO]()
      .routes(handler)
      .orNotFound
      .run(
        Request(method = Method.POST, uri = uri"/v1/provision")
          .withEntity(ProvisioningRequest(DescriptorKind.ComponentDescriptor, "a-yaml-descriptor"))
      )
    val expected = ProvisioningStatus(ProvisioningStatus.Status.Completed, "")

    check[ProvisioningStatus](response, Status.Ok, Some(expected)) shouldBe true
  }

  it should "return a 200 with COMPLETED on a successful provision with info" in {

    val resource: ImpalaEntityResource = ImpalaEntityResource(
      ExternalTable(
        "database",
        "tableName",
        Seq.empty,
        Seq.empty,
        "location",
        ImpalaFormat.Csv,
        None,
        Map.empty,
        header = false
      ),
      ImpalaCdpAcl.apply(Seq.empty, Seq.empty)
    )

    val controllerMock = new ProvisionerControllerMock {
      override def provision(request: ApiRequest.ProvisioningRequest)(implicit
          decoderPd: Decoder[ProvisioningDescriptor[Json]],
          decoderCmp: Decoder[Component[Json]]
      ): Either[ApiError, ApiResponse.ProvisioningStatus] =
        Right(ApiResponse.completed("a-fake-id", Some(toJson[ImpalaEntityResource](resource))))
    }
    val handler = new SpecificProvisionerHandler(controllerMock)
    val response: IO[Response[IO]] = new Resource[IO]()
      .routes(handler)
      .orNotFound
      .run(
        Request(method = Method.POST, uri = uri"/v1/provision")
          .withEntity(ProvisioningRequest(DescriptorKind.ComponentDescriptor, "a-yaml-descriptor"))
      )
    val expected = ProvisioningStatus(
      ProvisioningStatus.Status.Completed,
      "",
      Some(
        Info(
          publicInfo = parser
            .parse("""
            | {
            |   "impalaTable": { "type": "string", "label": "table", "value": "tableName" },
            |   "impalaDatabase": { "type": "string", "label": "database", "value": "database" },
            |   "impalaLocation": { "type": "string", "label": "location", "value": "location" },
            |   "impalaFormat": { "type": "string", "label": "format", "value": "CSV" }
            | }
            |""".stripMargin)
            .toOption
            .get,
          privateInfo = Json.obj()
        )
      )
    )

    check[ProvisioningStatus](response, Status.Ok, Some(expected)) shouldBe true
  }

  it should "return a 200 with COMPLETED on a successful provision output port view with info" in {

    val resource: ImpalaEntityResource = ImpalaEntityResource(
      ImpalaView(
        database = "database",
        name = "viewName",
        schema = Seq.empty,
        readsFromSourceName = Some("fromTable"),
        querySourceStatement = None
      ),
      ImpalaCdpAcl.apply(Seq.empty, Seq.empty)
    )

    val controllerMock = new ProvisionerControllerMock {
      override def provision(request: ApiRequest.ProvisioningRequest)(implicit
          decoderPd: Decoder[ProvisioningDescriptor[Json]],
          decoderCmp: Decoder[Component[Json]]
      ): Either[ApiError, ApiResponse.ProvisioningStatus] =
        Right(ApiResponse.completed("a-fake-id", Some(toJson[ImpalaEntityResource](resource))))
    }
    val handler = new SpecificProvisionerHandler(controllerMock)
    val response: IO[Response[IO]] = new Resource[IO]()
      .routes(handler)
      .orNotFound
      .run(
        Request(method = Method.POST, uri = uri"/v1/provision")
          .withEntity(ProvisioningRequest(DescriptorKind.ComponentDescriptor, "a-yaml-descriptor"))
      )
    val expected = ProvisioningStatus(
      ProvisioningStatus.Status.Completed,
      "",
      Some(
        Info(
          publicInfo = parser
            .parse("""
                | {
                |   "impalaDatabase": { "type": "string", "label": "database", "value": "database" },
                |   "impalaView": { "type": "string", "label": "view", "value": "viewName" }
                | }
                |""".stripMargin)
            .toOption
            .get,
          privateInfo = Json.obj()
        )
      )
    )

    check[ProvisioningStatus](response, Status.Ok, Some(expected)) shouldBe true
  }

  it should "return a 400 with a list of errors if an error happens on provision" in {
    val errors = Vector("first error", "second error")
    val controllerMock = new ProvisionerControllerMock {
      override def provision(request: ApiRequest.ProvisioningRequest)(implicit
          decoderPd: Decoder[ProvisioningDescriptor[Json]],
          decoderCmp: Decoder[Component[Json]]
      ): Either[ApiError, ApiResponse.ProvisioningStatus] = Left(ApiError.validErr(errors: _*))
    }

    val handler = new SpecificProvisionerHandler(controllerMock)
    val response: IO[Response[IO]] = new Resource[IO]()
      .routes(handler)
      .orNotFound
      .run(
        Request(method = Method.POST, uri = uri"/v1/provision")
          .withEntity(ProvisioningRequest(DescriptorKind.ComponentDescriptor, "a-yaml-descriptor"))
      )

    val expected = ValidationError(errors)

    check[ValidationError](response, Status.BadRequest, Some(expected)) shouldBe true
  }

  it should "return a 500 with meaningful error on provision exception" in {
    val error = "first error"
    val controllerMock = new ProvisionerControllerMock {
      override def provision(request: ApiRequest.ProvisioningRequest)(implicit
          decoderPd: Decoder[ProvisioningDescriptor[Json]],
          decoderCmp: Decoder[Component[Json]]
      ): Either[ApiError, ApiResponse.ProvisioningStatus] = Left(ApiError.sysErr(error))
    }

    val handler = new SpecificProvisionerHandler(controllerMock)
    val response: IO[Response[IO]] = new Resource[IO]()
      .routes(handler)
      .orNotFound
      .run(
        Request(method = Method.POST, uri = uri"/v1/provision")
          .withEntity(ProvisioningRequest(DescriptorKind.ComponentDescriptor, "a-yaml-descriptor"))
      )

    val expected = SystemError(error)

    check[SystemError](response, Status.InternalServerError, Some(expected)) shouldBe true
  }

  "The server" should "return a 200 with COMPLETED on a successful unprovision" in {
    val handler = new SpecificProvisionerHandler(new ProvisionerControllerMock)
    val response: IO[Response[IO]] = new Resource[IO]()
      .routes(handler)
      .orNotFound
      .run(
        Request(method = Method.POST, uri = uri"/v1/unprovision")
          .withEntity(ProvisioningRequest(DescriptorKind.ComponentDescriptor, "a-yaml-descriptor"))
      )
    val expected = ProvisioningStatus(ProvisioningStatus.Status.Completed, "")

    check[ProvisioningStatus](response, Status.Ok, Some(expected)) shouldBe true
  }

  it should "return a 400 with a list of errors if an error happens on unprovision" in {
    val errors = Vector("first error", "second error")
    val controllerMock = new ProvisionerControllerMock {
      override def unprovision(request: ApiRequest.ProvisioningRequest)(implicit
          decoderPd: Decoder[ProvisioningDescriptor[Json]],
          decoderCmp: Decoder[Component[Json]]
      ): Either[ApiError, ApiResponse.ProvisioningStatus] = Left(ApiError.validErr(errors: _*))
    }

    val handler = new SpecificProvisionerHandler(controllerMock)
    val response: IO[Response[IO]] = new Resource[IO]()
      .routes(handler)
      .orNotFound
      .run(
        Request(method = Method.POST, uri = uri"/v1/unprovision")
          .withEntity(ProvisioningRequest(DescriptorKind.ComponentDescriptor, "a-yaml-descriptor"))
      )

    val expected = ValidationError(errors)

    check[ValidationError](response, Status.BadRequest, Some(expected)) shouldBe true
  }

  it should "return a 500 with meaningful error on unprovision exception" in {
    val error = "first error"
    val controllerMock = new ProvisionerControllerMock {
      override def unprovision(request: ApiRequest.ProvisioningRequest)(implicit
          decoderPd: Decoder[ProvisioningDescriptor[Json]],
          decoderCmp: Decoder[Component[Json]]
      ): Either[ApiError, ApiResponse.ProvisioningStatus] = Left(ApiError.sysErr(error))
    }

    val handler = new SpecificProvisionerHandler(controllerMock)
    val response: IO[Response[IO]] = new Resource[IO]()
      .routes(handler)
      .orNotFound
      .run(
        Request(method = Method.POST, uri = uri"/v1/unprovision")
          .withEntity(ProvisioningRequest(DescriptorKind.ComponentDescriptor, "a-yaml-descriptor"))
      )

    val expected = SystemError(error)

    check[SystemError](response, Status.InternalServerError, Some(expected)) shouldBe true
  }

  it should "return a 200 with COMPLETED on successful updateAcl" in {
    val handler = new SpecificProvisionerHandler(new ProvisionerControllerMock)
    val response: IO[Response[IO]] = new Resource[IO]()
      .routes(handler)
      .orNotFound
      .run(
        Request(method = Method.POST, uri = uri"/v1/updateacl")
          .withEntity(
            UpdateAclRequest(
              Vector("user:user1", "group:agroup"),
              ProvisionInfo("a-yaml-descriptor", "")))
      )
    val expected = ProvisioningStatus(ProvisioningStatus.Status.Completed, "")

    check[ProvisioningStatus](response, Status.Ok, Some(expected)) shouldBe true
  }

  it should "return a 400 with a list of errors if an error happens on updateAcl" in {
    val errors = Vector("user doesnt exist", "group doesnt exist")
    val handler = new SpecificProvisionerHandler(new ProvisionerControllerMock {
      override def updateAcl(updateAclRequest: ApiRequest.UpdateAclRequest)(implicit
          decoderPd: Decoder[ProvisioningDescriptor[Json]],
          decoderCmp: Decoder[Component[Json]]
      ): Either[ApiError, ApiResponse.ProvisioningStatus] = Left(ApiError.validErr(errors: _*))
    })
    val response: IO[Response[IO]] = new Resource[IO]()
      .routes(handler)
      .orNotFound
      .run(
        Request(method = Method.POST, uri = uri"/v1/updateacl")
          .withEntity(
            UpdateAclRequest(
              Vector("user:user1", "group:agroup"),
              ProvisionInfo("a-yaml-descriptor", "")))
      )
    val expected = ValidationError(errors)

    check[ValidationError](response, Status.BadRequest, Some(expected)) shouldBe true
  }

  it should "return a 500 with a list of errors if an error happens on updateAcl" in {
    val error = "system error"
    val handler = new SpecificProvisionerHandler(new ProvisionerControllerMock {
      override def updateAcl(updateAclRequest: ApiRequest.UpdateAclRequest)(implicit
          decoderPd: Decoder[ProvisioningDescriptor[Json]],
          decoderCmp: Decoder[Component[Json]]
      ): Either[ApiError, ApiResponse.ProvisioningStatus] = Left(ApiError.sysErr(error))
    })
    val response: IO[Response[IO]] = new Resource[IO]()
      .routes(handler)
      .orNotFound
      .run(
        Request(method = Method.POST, uri = uri"/v1/updateacl")
          .withEntity(
            UpdateAclRequest(
              Vector("user:user1", "group:agroup"),
              ProvisionInfo("a-yaml-descriptor", "")))
      )
    val expected = SystemError(error)

    check[SystemError](response, Status.InternalServerError, Some(expected)) shouldBe true
  }

}
