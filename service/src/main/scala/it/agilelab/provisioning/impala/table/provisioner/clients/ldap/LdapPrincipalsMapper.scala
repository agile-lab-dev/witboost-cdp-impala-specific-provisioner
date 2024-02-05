package it.agilelab.provisioning.impala.table.provisioner.clients.ldap

import cats.implicits.toBifunctorOps
import it.agilelab.provisioning.commons.principalsmapping.PrincipalsMapperError.{
  PrincipalMappingError,
  PrincipalMappingSystemError
}
import it.agilelab.provisioning.commons.principalsmapping._

class LdapPrincipalsMapper(ldapClient: LdapClient) extends PrincipalsMapper[CdpIamPrincipals] {
  override def map(
      subjects: Set[String]
  ): Map[String, Either[PrincipalsMapperError, CdpIamPrincipals]] =
    subjects.map {
      case ref @ s"user:$user" =>
        val underscoreIndex = user.lastIndexOf("_")
        val userId =
          if (underscoreIndex.equals(-1)) user
          else s"${user.substring(0, underscoreIndex)}@${user.substring(underscoreIndex + 1)}"
        ref -> getAndMapUser(userId)

      case ref @ s"group:$group" => ref -> getAndMapGroup(group)

      case ref =>
        ref -> getAndMapUser(ref).left
          .flatMap(_ => getAndMapGroup(ref))
          .leftMap(_ =>
            PrincipalMappingError(
              ErrorMoreInfo(
                List(
                  s"Received principal '$ref' which doesn't exists as a group nor as an user on LDAP"),
                List.empty
              ),
              None
            ))
    }.toMap

  private def getAndMapUser(mail: String): Either[PrincipalsMapperError, CdpIamPrincipals] =
    ldapClient.findUserByMail(mail) match {
      case Right(Some(user)) => Right(user)
      case Right(None) =>
        Left(
          PrincipalMappingError(
            ErrorMoreInfo(
              List(s"Cannot find user '$mail' in LDAP system"),
              List.empty
            ),
            None
          )
        )
      case Left(iamErr) =>
        Left(
          PrincipalMappingSystemError(
            ErrorMoreInfo(List(s"Error while querying LDAP for user '$mail'"), List.empty),
            iamErr
          )
        )
    }

  private def getAndMapGroup(
      groupName: String
  ): Either[PrincipalsMapperError, CdpIamPrincipals] =
    ldapClient.findGroupByName(groupName) match {
      case Right(Some(group)) => Right(group)
      case Right(None) =>
        Left(
          PrincipalMappingError(
            ErrorMoreInfo(
              List(s"Cannot find group '$groupName' in LDAP system"),
              List.empty
            ),
            None
          )
        )
      case Left(iamErr) =>
        Left(
          PrincipalMappingSystemError(
            ErrorMoreInfo(List(s"Error while querying LDAP for group '$groupName'"), List.empty),
            iamErr
          )
        )
    }

}
