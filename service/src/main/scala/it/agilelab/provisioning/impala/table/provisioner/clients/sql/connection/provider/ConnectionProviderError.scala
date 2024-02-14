package it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider

import cats.Show
import cats.implicits.showInterpolator
import it.agilelab.provisioning.commons.showable.ShowableOps._
trait ConnectionProviderError extends Product with Serializable

object ConnectionProviderError {

  final case class ParseConnectionStringErr(connectionConfig: ConnectionConfig, pattern: String)
      extends ConnectionProviderError
  final case class GetConnectionErr(error: Throwable) extends ConnectionProviderError

  implicit def showConnectionProviderError: Show[ConnectionProviderError] = Show.show {
    case ParseConnectionStringErr(connectionConfig, pattern) =>
      show"ParseConnectionStringErr($connectionConfig, $pattern)"
    case GetConnectionErr(error) => show"GetConnectionErr($error)"
  }

}
