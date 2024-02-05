package it.agilelab.provisioning.impala.table.provisioner.clients.ldap

import cats.Show
import cats.implicits.showInterpolator
import it.agilelab.provisioning.commons.showable.ShowableOps._

sealed trait LdapClientError extends Exception with Product with Serializable

object LdapClientError {
  final case class FindPrincipalErr(subject: String, message: String, cause: Option[Throwable])
      extends LdapClientError {
    cause.foreach(initCause)
    override def getMessage: String = message
  }

  implicit val showLdapClientError: Show[LdapClientError] = Show.show {
    case FindPrincipalErr(subject, message, cause) =>
      show"FindPrincipalErr($subject, $message, ${cause.fold("")(e => show"$e")})"

  }
}
