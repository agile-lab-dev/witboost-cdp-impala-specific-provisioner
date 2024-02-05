package it.agilelab.provisioning.impala.table.provisioner.clients.ldap

import it.agilelab.provisioning.commons.audit.Audit
import it.agilelab.provisioning.commons.principalsmapping.{ CdpIamGroup, CdpIamUser }
import it.agilelab.provisioning.impala.table.provisioner.clients.ldap.LdapClientError.FindPrincipalErr
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

class DefaultLdapClientWithAuditTest extends AnyFunSuite with MockFactory {

  val defaultLdapClient = stub[LdapClient]
  val audit: Audit = mock[Audit]
  val ldapClient = new DefaultLdapClientWithAudit(audit, defaultLdapClient)

  test("findUserByMail logs success info") {
    val ret = CdpIamUser("userId", "john.doe@company.com", "")
    (defaultLdapClient.findUserByMail _)
      .when("john.doe@company.com")
      .returns(Right(Some(ret)))

    inSequence(
      (audit.info _).expects("Executing FindUserByMail(john.doe@company.com)"),
      (audit.info _).expects("FindUserByMail(john.doe@company.com) completed successfully")
    )
    val actual = ldapClient.findUserByMail("john.doe@company.com")
    assert(actual == Right(Some(ret)))
  }

  test("findUserByMail logs error info") {
    val error = FindPrincipalErr(
      "john.doe@company.com",
      "An error occurred while searching for the user 'john.doe@company.com' on LDAP. Please try again and if the error persists contact the platform team. Details: ",
      None
    )

    (defaultLdapClient.findUserByMail _)
      .when("john.doe@company.com")
      .returns(Left(error))

    inSequence(
      (audit.info _).expects("Executing FindUserByMail(john.doe@company.com)"),
      (audit.error _).expects(
        "FindUserByMail(john.doe@company.com) failed. Details: FindPrincipalErr(john.doe@company.com, An error occurred while searching for the user 'john.doe@company.com' on LDAP. Please try again and if the error persists contact the platform team. Details: , )")
    )
    val actual = ldapClient.findUserByMail("john.doe@company.com")
    assert(actual == Left(error))
  }

  test("findGroupByName logs success info") {
    val ret = CdpIamGroup("groupName", "")
    (defaultLdapClient.findGroupByName _)
      .when("groupName")
      .returns(Right(Some(ret)))

    inSequence(
      (audit.info _).expects("Executing FindGroupByName(groupName)"),
      (audit.info _).expects("FindGroupByName(groupName) completed successfully")
    )
    val actual = ldapClient.findGroupByName("groupName")
    assert(actual == Right(Some(ret)))
  }

  test("findGroupByName logs error info") {
    val error = FindPrincipalErr(
      "groupName",
      "An error occurred while searching for the group 'groupName' on LDAP. Please try again and if the error persists contact the platform team. Details: ",
      None
    )

    (defaultLdapClient.findGroupByName _)
      .when("groupName")
      .returns(Left(error))

    inSequence(
      (audit.info _).expects("Executing FindGroupByName(groupName)"),
      (audit.error _).expects(
        "FindGroupByName(groupName) failed. Details: FindPrincipalErr(groupName, An error occurred while searching for the group 'groupName' on LDAP. Please try again and if the error persists contact the platform team. Details: , )")
    )
    val actual = ldapClient.findGroupByName("groupName")
    assert(actual == Left(error))
  }

}
