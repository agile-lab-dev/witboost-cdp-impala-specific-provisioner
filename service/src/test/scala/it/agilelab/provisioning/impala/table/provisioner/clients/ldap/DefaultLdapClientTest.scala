package it.agilelab.provisioning.impala.table.provisioner.clients.ldap

import it.agilelab.provisioning.commons.principalsmapping.{ CdpIamGroup, CdpIamUser }
import it.agilelab.provisioning.impala.table.provisioner.clients.ldap.LdapClientError.FindPrincipalErr
import org.ldaptive._
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar

// Testing with Mockito as ScalaMock doesn't behave well with Java classes that call methods inside its constructor
class DefaultLdapClientTest extends AnyFunSuite with MockitoSugar with BeforeAndAfterEach {

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

  val searchOperation = mock[SearchOperation]

  val ldapClient = new DefaultLdapClient(searchOperation, config)

  override protected def beforeEach(): Unit = Mockito.reset(searchOperation)

  test("findUserByMail should return Right(Some(CdpIamUser))") {
    Mockito
      .when(searchOperation.execute(any[SearchRequest]))
      .thenReturn(
        SearchResponse
          .builder()
          .entry(
            LdapEntry
              .builder()
              .attributes(
                LdapAttribute.builder().name("mail").values("john.doe@company.com").build(),
                LdapAttribute.builder().name("cn").values("userId").build())
              .build())
          .build())

    val expected = CdpIamUser("userId", "userId", "")
    val actual = ldapClient.findUserByMail("john.doe@company.com")

    assert(Right(Some(expected)) == actual)
  }

  test("findUserByMail should return Right(Some(None))") {
    Mockito
      .when(searchOperation.execute(any[SearchRequest]))
      .thenReturn(
        SearchResponse
          .builder()
          .build()
      )
    val actual = ldapClient.findUserByMail("john.doe@company.com")
    assert(actual == Right(None))
  }

  test("findUserByMail should return Left(GetPrincipalErr)") {
    val err = new LdapException("Error!")
    Mockito
      .when(searchOperation.execute(any[SearchRequest]))
      .thenThrow(err)

    val actual = ldapClient.findUserByMail("john.doe@company.com")
    assert(
      actual == Left(FindPrincipalErr(
        "john.doe@company.com",
        "An error occurred while searching for the user 'john.doe@company.com' on LDAP. Please try again and if the error persists contact the platform team. Details: Error!",
        Some(err)
      )))
  }

  test("findGroupByName should return Right(Some(CdpIamGroup))") {
    Mockito
      .when(searchOperation.execute(any[SearchRequest]))
      .thenReturn(
        SearchResponse
          .builder()
          .entry(
            LdapEntry
              .builder()
              .attributes(LdapAttribute.builder().name("cn").values("groupName").build())
              .build())
          .build())

    val expected = CdpIamGroup("groupName", "")
    val actual = ldapClient.findGroupByName("groupName")

    assert(Right(Some(expected)) == actual)
  }

  test("findGroupByName should return Right(Some(None))") {
    Mockito
      .when(searchOperation.execute(any[SearchRequest]))
      .thenReturn(
        SearchResponse
          .builder()
          .build()
      )
    val actual = ldapClient.findGroupByName("groupName")
    assert(actual == Right(None))
  }

  test("findGroupByName should return Left(GetPrincipalErr)") {
    val err = new LdapException("Error!")
    Mockito
      .when(searchOperation.execute(any[SearchRequest]))
      .thenThrow(err)

    val actual = ldapClient.findGroupByName("groupName")
    assert(
      actual == Left(FindPrincipalErr(
        "groupName",
        "An error occurred while searching for the group 'groupName' on LDAP. Please try again and if the error persists contact the platform team. Details: Error!",
        Some(err)
      )))
  }
}
