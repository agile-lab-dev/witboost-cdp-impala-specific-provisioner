package it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider

import cats.implicits.toBifunctorOps
import com.typesafe.config.Config
import it.agilelab.provisioning.commons.config.ConfError
import it.agilelab.provisioning.commons.config.ConfError.ConfKeyNotFoundErr
import it.agilelab.provisioning.impala.table.provisioner.context.ApplicationConfiguration
import it.agilelab.provisioning.impala.table.provisioner.context.ContextError.ConfigurationError
import it.agilelab.provisioning.mesh.self.service.core.gateway.ComponentGatewayError

import scala.util.Try

final case class ConnectionConfig(
    host: String,
    port: String,
    schema: String,
    user: String,
    password: String
)

object ConnectionConfig {

  /** Initializes a ConnectionConfig based on a given configuration, allowing to override fields by passing them as parameters
    * @param config Config in the shape { port: int, schema: int }
    * @param host Connection host
    * @param port Optional Connection port, if not given it will be retrieved from config
    * @param schema Optional Schema, if not given it will be retrieve from config
    * @return Left([[ConfError]]) when a not given connection configuration is not found on the config object,
    *         Right([[ConnectionConfig]]) with the connection string parameters if retrieved correctly
    */
  def getFromConfig(
      config: Config,
      host: String,
      port: Option[Int] = None,
      schema: Option[String] = None
  ): Either[ConfError, ConnectionConfig] = for {
    impalaPort <- port.fold(Try {
      config.getInt(ApplicationConfiguration.IMPALA_PORT)
    }.toEither.leftMap { _ =>
      ConfKeyNotFoundErr(ApplicationConfiguration.IMPALA_PORT)
    })(Right(_))
    impalaSchema <- schema.fold(Try {
      config.getString(ApplicationConfiguration.IMPALA_SCHEMA)
    }.toEither.leftMap { _ =>
      ConfKeyNotFoundErr(ApplicationConfiguration.IMPALA_SCHEMA)
    })(Right(_))
  } yield ConnectionConfig(host, impalaPort.toString, impalaSchema, "", "")
}
