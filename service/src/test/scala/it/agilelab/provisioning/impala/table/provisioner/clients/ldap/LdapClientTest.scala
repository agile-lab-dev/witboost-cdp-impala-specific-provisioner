package it.agilelab.provisioning.impala.table.provisioner.clients.ldap

import com.typesafe.config.ConfigFactory
import org.ldaptive.SearchOperation
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar

// Testing with Mockito as ScalaMock doesn't behave well with Java classes that call methods inside its constructor
class LdapClientTest extends AnyFunSuite with MockitoSugar {

  test("default") {
    val searchOperation = mock[SearchOperation]
    val config = LdapConfig(
      url = "ldap://localhost:389",
      useTls = false,
      timeout = 30000,
      bindUsername = "user",
      bindPassword = "pwd",
      searchBaseDN = "DC=company,DC=com",
      userSearchFilter = "(mail={mail})",
      groupSearchFilter = "(&(objectClass=groupOfNames)(cn={group}))",
      userAttributeName = "cn",
      groupAttributeName = "cn"
    )
    val actual = LdapClient.default(searchOperation, config)
    assert(actual.isInstanceOf[DefaultLdapClient])
  }

  test("default with audit") {
    val searchOperation = mock[SearchOperation]
    val config = LdapConfig(
      url = "ldap://localhost:389",
      useTls = false,
      timeout = 30000,
      bindUsername = "user",
      bindPassword = "pwd",
      searchBaseDN = "DC=company,DC=com",
      userSearchFilter = "(mail={mail})",
      groupSearchFilter = "(&(objectClass=groupOfNames)(cn={group}))",
      userAttributeName = "cn",
      groupAttributeName = "cn"
    )
    val actual = LdapClient.defaultWithAudit(searchOperation, config)
    assert(actual.isInstanceOf[DefaultLdapClientWithAudit])
  }

  test("ldap from config") {
    val config =
      """
        |  {
        |      "url": "ldap://localhost:389",
        |      "useTls": "false",
        |      "timeout": "30000",
        |      "bindUsername": "user",
        |      "bindPassword": "pwd",
        |      "searchBaseDN": "DC=company,DC=com",
        |      "userSearchFilter": "(mail={mail})",
        |      "groupSearchFilter": "(&(objectClass=groupOfNames)(cn={group}))",
        |      "userAttributeName": "cn",
        |      "groupAttributeName": "cn",
        |    }
        |""".stripMargin

    val expected = LdapConfig(
      url = "ldap://localhost:389",
      useTls = false,
      timeout = 30000,
      bindUsername = "user",
      bindPassword = "pwd",
      searchBaseDN = "DC=company,DC=com",
      userSearchFilter = "(mail={mail})",
      groupSearchFilter = "(&(objectClass=groupOfNames)(cn={group}))",
      userAttributeName = "cn",
      groupAttributeName = "cn"
    )

    val actual = LdapConfig.fromConfig(ConfigFactory.parseString(config))
    assert(actual == Right(expected))
  }

  test("ldap fail from config") {
    val config =
      """
        |  {
        |      "url": "ldap://incomplete.configuratio:389",
        |      "groupSearchFilter": "(&(objectClass=groupOfNames)(cn={group}))",
        |    }
        |""".stripMargin

    val actual = LdapConfig.fromConfig(ConfigFactory.parseString(config))
    assert(actual.isLeft)
  }

}
