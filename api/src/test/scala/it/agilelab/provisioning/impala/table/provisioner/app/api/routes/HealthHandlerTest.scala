package it.agilelab.provisioning.impala.table.provisioner.app.api.routes

import cats.effect.IO
import it.agilelab.provisioning.impala.table.provisioner.app.api.routes.helpers.HandlerTestBase
import org.http4s.{ Method, Request, Response, Status }
import org.http4s.implicits.http4sLiteralsSyntax

class HealthHandlerTest extends HandlerTestBase {

  "The server" should "return a simple health check response " in {
    val response: IO[Response[IO]] = HealthCheck
      .routes[IO]()
      .orNotFound
      .run(
        Request(method = Method.GET, uri = uri"health")
      )

    check[String](response, Status.Ok, None) shouldBe true
  }

}
