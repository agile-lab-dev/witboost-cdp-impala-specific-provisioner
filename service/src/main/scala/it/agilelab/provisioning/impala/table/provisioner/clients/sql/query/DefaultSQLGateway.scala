package it.agilelab.provisioning.impala.table.provisioner.clients.sql.query

import cats.implicits._
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider.{
  ConnectionConfig,
  ConnectionProvider
}
import SqlGatewayError.ExecuteDDLErr
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider.{
  ConnectionConfig,
  ConnectionProvider
}

import scala.util.Using

class DefaultSQLGateway(connectionProvider: ConnectionProvider) extends SqlGateway {

  override def executeDDL(
      connectionConfig: ConnectionConfig,
      ddl: String
  ): Either[SqlGatewayError, Int] =
    Using(connectionProvider.get(connectionConfig)) { c =>
      c.createStatement().executeUpdate(ddl)
    }.toEither.leftMap(e => ExecuteDDLErr(e))

  override def executeDDLs(
      connectionConfig: ConnectionConfig,
      ddls: Seq[String]
  ): Either[SqlGatewayError, Int] =
    Using(connectionProvider.get(connectionConfig)) { c =>
      ddls.map(c.createStatement().executeUpdate).sum
    }.toEither.leftMap(e => ExecuteDDLErr(e))
}
