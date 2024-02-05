package it.agilelab.provisioning.impala.table.provisioner.clients.ldap

import cats.implicits.showInterpolator
import it.agilelab.provisioning.commons.audit.Audit
import it.agilelab.provisioning.commons.principalsmapping.{ CdpIamGroup, CdpIamUser }

class DefaultLdapClientWithAudit(audit: Audit, ldapClient: LdapClient) extends LdapClient {
  private val INFO_MSG = "Executing %s"
  override def findUserByMail(mail: String): Either[LdapClientError, Option[CdpIamUser]] = {
    val action = s"FindUserByMail($mail)"
    audit.info(INFO_MSG.format(action))
    val result = ldapClient.findUserByMail(mail)
    auditWithinResult(result, action)
    result
  }

  override def findGroupByName(name: String): Either[LdapClientError, Option[CdpIamGroup]] = {
    val action = s"FindGroupByName($name)"
    audit.info(INFO_MSG.format(action))
    val result = ldapClient.findGroupByName(name)
    auditWithinResult(result, action)
    result
  }

  private def auditWithinResult[D](
      result: Either[LdapClientError, D],
      action: String
  ): Unit =
    result match {
      case Right(_) => audit.info(show"$action completed successfully")
      case Left(l)  => audit.error(show"$action failed. Details: $l")
    }
}
