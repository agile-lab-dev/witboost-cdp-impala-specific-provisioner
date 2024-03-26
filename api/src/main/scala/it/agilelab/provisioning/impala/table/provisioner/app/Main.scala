package it.agilelab.provisioning.impala.table.provisioner.app
import cats.effect.{ ExitCode, IO, IOApp }
import cats.implicits.{ showInterpolator, toBifunctorOps }
import com.comcast.ip4s.{ Host, Port }
import com.typesafe.scalalogging.StrictLogging
import it.agilelab.provisioning.aws.s3.gateway.S3GatewayError.S3GatewayInitError
import it.agilelab.provisioning.commons.client.cdp.dl.CdpDlClientError.CdpDlClientInitErr
import it.agilelab.provisioning.commons.client.cdp.dw.CdpDwClientError.CdpDwClientInitClientError
import it.agilelab.provisioning.commons.client.cdp.env.CdpEnvClientError.CdpEnvClientInitError
import it.agilelab.provisioning.commons.config.{ Conf, ConfError }
import it.agilelab.provisioning.impala.table.provisioner.app.config.{
  FrameworkDependencies,
  ImpalaProvisionerController,
  RetryPolicyBuilder
}
import it.agilelab.provisioning.impala.table.provisioner.context.ApplicationConfiguration
import it.agilelab.provisioning.impala.table.provisioner.context.ContextError.{
  ClientError,
  ConfigurationError,
  PrincipalsMapperLoaderError
}
import org.http4s.ember.server.EmberServerBuilder

object Main extends IOApp with StrictLogging {

  private def logThrowableError(throwable: Throwable): Unit =
    throwable match {
      case err: S3GatewayInitError         => logger.error("S3GatewayInitError", err.error)
      case err: CdpDlClientInitErr         => logger.error("CdpDlClientInitErr", err.error)
      case err: CdpEnvClientInitError      => logger.error("CdpEnvClientInitError", err.error)
      case err: CdpDwClientInitClientError => logger.error("CdpDwClientInitClientError", err.error)
      case err: ConfError                  => logger.error(show"$err", err)
      case th                              => logger.error("Generic throwable error", th)
    }

  val conf: Conf = Conf.envWithAudit()
  override def run(args: List[String]): IO[ExitCode] = for {
    provisionerController <- ImpalaProvisionerController(conf) match {
      case Left(error: ClientError) =>
        logThrowableError(error.throwable)
        IO.raiseError(error.throwable)
      case Left(error: ConfigurationError) =>
        logThrowableError(error.error)
        IO.raiseError(error.error)
      case Left(error: PrincipalsMapperLoaderError) =>
        logThrowableError(error.throwable)
        IO.raiseError(error.throwable)
      case Right(value) => IO.pure(value)
    }
    provisionerConfig = ApplicationConfiguration.provisionerConfig
    frameworkDependencies <- IO.fromEither {
      RetryPolicyBuilder
        .applyFromConfig[IO](
          provisionerConfig.getConfig(ApplicationConfiguration.PROVISION_RETRY_CONFIG))
        .leftMap(err => err.error)
        .map(retryPolicy => new FrameworkDependencies(provisionerController, retryPolicy))
    }
    interface <- IO.fromOption(
      Host
        .fromString(
          provisionerConfig.getString(ApplicationConfiguration.NETWORKING_HTTPSERVER_INTERFACE)))(
      new RuntimeException("Interface not valid"))
    port <- IO.fromOption(
      Port
        .fromInt(provisionerConfig.getInt(ApplicationConfiguration.NETWORKING_HTTPSERVER_PORT)))(
      new RuntimeException("Port not valid"))
    server <- EmberServerBuilder
      .default[IO]
      .withPort(port)
      .withHost(interface)
      .withHttpApp(frameworkDependencies.httpApp)
      .build
      .useForever
      .as(ExitCode.Success)
  } yield server

}
