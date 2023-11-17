package it.agilelab.provisioning.impala.table.provisioner.app
import cats.effect.{ ExitCode, IO, IOApp }
import com.comcast.ip4s.{ Host, Port }
import it.agilelab.provisioning.commons.config.Conf
import it.agilelab.provisioning.impala.table.provisioner.app.config.{
  ApplicationConfiguration,
  FrameworkDependencies,
  ImpalaProvisionerController
}
import it.agilelab.provisioning.impala.table.provisioner.context.ContextError.{
  ClientError,
  ConfigurationError
}
import org.http4s.ember.server.EmberServerBuilder

object Main extends IOApp {

  val conf: Conf = Conf.envWithAudit()
  override def run(args: List[String]): IO[ExitCode] = for {
    provisionerController <- ImpalaProvisionerController(conf) match {
      case Left(error: ClientError)        => IO.raiseError(error.throwable)
      case Left(error: ConfigurationError) => IO.raiseError(error.error)
      case Right(value)                    => IO.pure(value)
    }
    frameworkDependencies <- IO.pure(new FrameworkDependencies(provisionerController))
    interface <- IO.fromOption(
      Host
        .fromString(ApplicationConfiguration.provisionerConfig.getString(
          ApplicationConfiguration.NETWORKING_HTTPSERVER_INTERFACE)))(
      new RuntimeException("Interface not valid"))
    port <- IO.fromOption(
      Port
        .fromInt(ApplicationConfiguration.provisionerConfig.getInt(
          ApplicationConfiguration.NETWORKING_HTTPSERVER_PORT)))(
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
