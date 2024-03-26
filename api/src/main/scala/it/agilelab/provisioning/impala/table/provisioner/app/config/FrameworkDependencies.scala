package it.agilelab.provisioning.impala.table.provisioner.app.config

import cats.data.Kleisli
import cats.effect.IO
import cats.implicits.toSemigroupKOps
import io.circe.Json
import it.agilelab.provisioning.api.generated.{ Handler, Resource }
import it.agilelab.provisioning.commons.principalsmapping.CdpIamPrincipals
import it.agilelab.provisioning.impala.table.provisioner.app.api.SpecificProvisionerHandler
import it.agilelab.provisioning.impala.table.provisioner.app.api.routes.HealthCheck
import it.agilelab.provisioning.mesh.self.service.api.controller.ProvisionerController
import org.http4s.server.middleware.Logger
import org.http4s.{ Request, Response }
import retry.RetryPolicy

final class FrameworkDependencies(
    provisionerController: ProvisionerController[Json, Json, CdpIamPrincipals],
    retryPolicy: RetryPolicy[IO]
) {

  private val provisionerHandler: Handler[IO] =
    new SpecificProvisionerHandler(provisionerController, retryPolicy)
  private val provisionerService = new Resource[IO]().routes(provisionerHandler)
  private val combinedServices = HealthCheck.routes[IO]() <+> provisionerService

  private val withloggerService = Logger.httpRoutes[IO](
    logHeaders = false,
    logBody = true,
    redactHeadersWhen = _ => false,
    logAction = None
  )(combinedServices)

  val httpApp: Kleisli[IO, Request[IO], Response[IO]] = withloggerService.orNotFound
}
