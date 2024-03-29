package it.agilelab.provisioning.impala.table.provisioner.clients.sql.query

import cats.Show
import cats.implicits._
import it.agilelab.provisioning.commons.showable.ShowableOps.showThrowableError
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider.ConnectionProviderError

trait SqlGatewayError extends Exception with Product with Serializable

object SqlGatewayError {
  final case class ExecuteDDLErr(error: Throwable) extends SqlGatewayError
  final case class ConnectionErr(error: ConnectionProviderError) extends SqlGatewayError

  implicit def showSqlGatewayError: Show[SqlGatewayError] = Show.show {
    case ExecuteDDLErr(error) => show"ExecuteDDLErr($error)"
    case ConnectionErr(error) => show"ConnectionErr($error)"
  }
}
