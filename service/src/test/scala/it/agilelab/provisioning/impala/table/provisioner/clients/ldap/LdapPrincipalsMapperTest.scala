package it.agilelab.provisioning.impala.table.provisioner.clients.ldap

import it.agilelab.provisioning.commons.principalsmapping.PrincipalsMapperError.{
  PrincipalMappingError,
  PrincipalMappingSystemError
}
import it.agilelab.provisioning.commons.principalsmapping.{ CdpIamGroup, CdpIamUser, ErrorMoreInfo }
import it.agilelab.provisioning.impala.table.provisioner.clients.ldap.LdapClientError.FindPrincipalErr
import org.ldaptive.LdapException
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

class LdapPrincipalsMapperTest extends AnyFunSuite with MockFactory {

  val ldapClient: LdapClient = mock[LdapClient]
  val principalsMapper = new LdapPrincipalsMapper(ldapClient)

  test("map successfully users and groups") {
    val refs = Set(
      "user:john.doe_company.com",
      "group:data-eng"
    )

    val expected = Map(
      "user:john.doe_company.com" -> Right(CdpIamUser("john.doe1", "john.doe@company.com", "")),
      "group:data-eng"            -> Right(CdpIamGroup("data-eng", ""))
    )

    (ldapClient.findUserByMail _)
      .expects("john.doe@company.com")
      .once()
      .returns(Right(Some(CdpIamUser("john.doe1", "john.doe@company.com", ""))))
    (ldapClient.findGroupByName _)
      .expects("data-eng")
      .once()
      .returns(Right(Some(CdpIamGroup("data-eng", ""))))

    val actual = principalsMapper.map(refs)
    assert(actual == expected)
  }

  test("map wrong users or groups") {
    val refs = Set(
      "user:john.doe_company.com",
      "group:data-eng",
      "group:ldap-crashed",
      "i-will-error"
    )
    val sysErr = FindPrincipalErr("ldap-crashed", "Crash!", Some(new LdapException("Crash!")))

    val expected = Map(
      "user:john.doe_company.com" -> Left(
        PrincipalMappingError(
          ErrorMoreInfo(
            List(s"Cannot find user 'john.doe@company.com' in LDAP system"),
            List.empty
          ),
          None
        )),
      "group:data-eng" -> Left(
        PrincipalMappingError(
          ErrorMoreInfo(
            List(s"Cannot find group 'data-eng' in LDAP system"),
            List.empty
          ),
          None
        )
      ),
      "group:ldap-crashed" -> Left(
        PrincipalMappingSystemError(
          ErrorMoreInfo(List(s"Error while querying LDAP for group 'ldap-crashed'"), List.empty),
          sysErr
        )
      ),
      "i-will-error" -> Left(
        PrincipalMappingError(
          ErrorMoreInfo(
            List(
              s"Received principal 'i-will-error' which doesn't exists as a group nor as an user on LDAP"),
            List.empty
          ),
          None
        ))
    )

    (ldapClient.findUserByMail _)
      .expects("john.doe@company.com")
      .once()
      .returns(Right(None))
    (ldapClient.findGroupByName _)
      .expects("data-eng")
      .once()
      .returns(Right(None))
    (ldapClient.findGroupByName _)
      .expects("ldap-crashed")
      .once()
      .returns(Left(sysErr))
    (ldapClient.findUserByMail _)
      .expects("i-will-error")
      .once()
      .returns(Right(None))
    (ldapClient.findGroupByName _)
      .expects("i-will-error")
      .once()
      .returns(Right(None))

    val actual = principalsMapper.map(refs)
    assert(actual == expected)
  }

}
