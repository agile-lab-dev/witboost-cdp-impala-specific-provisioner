package it.agilelab.provisioning.impala.table.provisioner.app.api.routes.helpers

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import com.typesafe.scalalogging.StrictLogging
import org.http4s.{ EntityDecoder, Response, Status }
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HandlerTestBase extends AnyFlatSpec with Matchers with StrictLogging {

  implicit val runtime: IORuntime = cats.effect.unsafe.IORuntime.global

  def check[A](actual: IO[Response[IO]], expectedStatus: Status, expectedBody: Option[A])(implicit
      ev: EntityDecoder[IO, A]
  ): Boolean = {
    val actualResp = actual.unsafeRunSync()
    val statusCheck = actualResp.status == expectedStatus
    val bodyCheck = expectedBody.fold[Boolean](
      // Verify Response's body is empty.
      actualResp.body.compile.toVector.unsafeRunSync().isEmpty
    ) { expected =>
      val x = actualResp.as[A].unsafeRunSync()
      logger.info("Received {}. Expected {}", x, expected)
      x == expected
    }
    statusCheck && bodyCheck
  }

  def check[A](actual: IO[Response[IO]], expectedStatus: Status)(implicit
      ev: EntityDecoder[IO, A]
  ): Boolean = {
    val actualResp = actual.unsafeRunSync()
    val statusCheck = actualResp.status == expectedStatus
    val bodyCheck = actualResp.as[A].unsafeRunSync().isInstanceOf[A]
    statusCheck && bodyCheck
  }

}
