package it.agilelab.provisioning.impala.table.provisioner.clients.ldap
import cats.implicits.toBifunctorOps
import it.agilelab.provisioning.commons.principalsmapping.{ CdpIamGroup, CdpIamUser }
import it.agilelab.provisioning.impala.table.provisioner.clients.ldap.LdapClientError.FindPrincipalErr
import org.ldaptive.{ FilterTemplate, SearchOperation, SearchRequest }

import scala.util.Try

class DefaultLdapClient(searchOperation: SearchOperation, ldapConfig: LdapConfig)
    extends LdapClient {
  override def findUserByMail(mail: String): Either[LdapClientError, Option[CdpIamUser]] = {
    val filterTemplate: FilterTemplate = FilterTemplate.builder
      .filter(ldapConfig.userSearchFilter)
      .parameter("mail", mail)
      .build
    val searchRequest: SearchRequest = SearchRequest.builder
      .dn(ldapConfig.searchBaseDN)
      .filter(filterTemplate)
      .build

    Try {
      val searchResponse = searchOperation.execute(searchRequest)
      val entry = searchResponse.getEntry
      if (entry != null) {
        val userAttributeName = entry.getAttribute(ldapConfig.userAttributeName).getStringValue
        Some(CdpIamUser(userAttributeName, userAttributeName, ""))
      } else {
        None
      }
    }.toEither.leftMap { e =>
      val errorMessage = String.format(
        "An error occurred while searching for the user '%s' on LDAP. Please try again and if the error persists contact the platform team. Details: %s",
        mail,
        e.getMessage
      )
      FindPrincipalErr(mail, errorMessage, Some(e))
    }
  }

  override def findGroupByName(name: String): Either[LdapClientError, Option[CdpIamGroup]] = {
    val filterTemplate = FilterTemplate.builder
      .filter(ldapConfig.groupSearchFilter)
      .parameter("group", name)
      .build
    val searchRequest = SearchRequest.builder
      .dn(ldapConfig.searchBaseDN)
      .filter(filterTemplate)
      .build

    Try {
      val searchResponse = searchOperation.execute(searchRequest)
      val entry = searchResponse.getEntry
      if (entry != null) {
        val group = entry.getAttribute(ldapConfig.groupAttributeName).getStringValue
        Some(CdpIamGroup(group, ""))
      } else {
        None
      }
    }.toEither.leftMap { e =>
      val errorMessage = String.format(
        "An error occurred while searching for the group '%s' on LDAP. Please try again and if the error persists contact the platform team. Details: %s",
        name,
        e.getMessage
      )
      FindPrincipalErr(name, errorMessage, Some(e))
    }
  }
}
