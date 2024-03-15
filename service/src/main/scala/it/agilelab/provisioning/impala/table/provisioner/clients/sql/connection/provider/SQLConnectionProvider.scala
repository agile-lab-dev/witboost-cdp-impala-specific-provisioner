package it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider

import cats.implicits.toBifunctorOps
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider.ConnectionProviderError.GetConnectionErr

import java.sql.{ Connection, DriverManager }
import scala.util.Try

class SQLConnectionProvider(
    driver: String,
    sqlConnectionStringProvider: ConnectionStringProvider
) extends ConnectionProvider {

  override def get(
      connectionConfig: ConnectionConfig
  ): Either[ConnectionProviderError, Connection] =
    for {
      jdbcConnectionString <- getConnectionString(connectionConfig)
      connection <-
        Try {
          Class.forName(driver)
          DriverManager.getConnection(jdbcConnectionString)
        }.toEither.leftMap(GetConnectionErr)
    } yield connection

  override def getConnectionString(
      connectionConfig: ConnectionConfig
  ): Either[ConnectionProviderError, String] = sqlConnectionStringProvider.get(connectionConfig)
}
