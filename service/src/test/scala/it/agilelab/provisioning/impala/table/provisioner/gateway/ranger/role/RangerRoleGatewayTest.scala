package it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.role

import it.agilelab.provisioning.commons.client.ranger.RangerClient
import it.agilelab.provisioning.commons.client.ranger.model.{ RangerRole, RoleMember }
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

class RangerRoleGatewayTest extends AnyFunSuite with MockFactory {

  test("upsertRole creates new owner role") {
    val rangerClient = mock[RangerClient]
    val gateway = new RangerRoleGateway(
      rangerClient
    )

    val role = RangerRole(
      id = 0,
      isEnabled = true,
      name = "prefix1_owner",
      description = "",
      groups = Seq(RoleMember("group1", isAdmin = false)),
      users = Seq(
        RoleMember("user1", isAdmin = false),
        RoleMember("deployUser", isAdmin = true),
        RoleMember("owner1", isAdmin = true)
      ),
      roles = Seq.empty
    )

    val created = role.copy(id = 144)

    (rangerClient.findRoleByName _)
      .expects("prefix1_owner")
      .once()
      .returns(Right(None))

    (rangerClient.createRole _)
      .expects(role)
      .once()
      .returns(Right(created))

    val actual = gateway.upsertRole(
      rolePrefix = "prefix1",
      roleType = OwnerRoleType,
      deployUser = "deployUser",
      ownerUsers = Seq("owner1"),
      ownerGroups = Seq.empty,
      users = Seq("user1"),
      groups = Seq("group1")
    )

    val expected = Right(created)

    assert(actual.isRight)
    assert(actual == expected)
  }

  test("upsertRole updates existing owner role preserving admins") {
    val rangerClient = mock[RangerClient]
    val gateway = new RangerRoleGateway(
      rangerClient
    )

    val role = RangerRole(
      id = 111,
      isEnabled = true,
      name = "prefix1_owner",
      description = "",
      groups = Seq(RoleMember("group1", isAdmin = false)),
      users = Seq(
        RoleMember("user1", isAdmin = false),
        RoleMember("deployUser", isAdmin = true),
        RoleMember("owner1", isAdmin = true)
      ),
      roles = Seq.empty
    )

    val updatedRole = role.copy(
      users = Seq(
        RoleMember("user2", isAdmin = false),
        RoleMember("deployUser2", isAdmin = true),
        RoleMember("owner1", isAdmin = true),
        RoleMember("deployUser", isAdmin = true)
      ),
      groups = Seq(RoleMember("group1", isAdmin = false), RoleMember("ownerGroup2", isAdmin = true))
    )

    (rangerClient.findRoleByName _)
      .expects("prefix1_owner")
      .once()
      .returns(Right(Some(role)))

    (rangerClient.updateRole _)
      .expects(updatedRole)
      .once()
      .returns(Right(updatedRole))

    val actual = gateway.upsertRole(
      rolePrefix = "prefix1",
      roleType = OwnerRoleType,
      deployUser = "deployUser2",
      ownerUsers = Seq("owner1"),
      ownerGroups = Seq("ownerGroup2"),
      users = Seq("user2"),
      groups = Seq("group1")
    )

    val expected = Right(updatedRole)

    assert(actual.isRight)
    assert(actual == expected)
  }

  test("upsertRole creates new user role") {
    val rangerClient = mock[RangerClient]
    val gateway = new RangerRoleGateway(
      rangerClient
    )

    val role = RangerRole(
      id = 0,
      isEnabled = true,
      name = "prefix1_read",
      description = "",
      groups = Seq(RoleMember("group1", isAdmin = false)),
      users = Seq(
        RoleMember("user1", isAdmin = false),
        RoleMember("deployUser", isAdmin = true),
        RoleMember("owner1", isAdmin = true)
      ),
      roles = Seq.empty
    )

    val created = role.copy(id = 144)

    (rangerClient.findRoleByName _)
      .expects("prefix1_read")
      .once()
      .returns(Right(None))

    (rangerClient.createRole _)
      .expects(role)
      .once()
      .returns(Right(created))

    val actual = gateway.upsertRole(
      rolePrefix = "prefix1",
      roleType = UserRoleType,
      deployUser = "deployUser",
      ownerUsers = Seq("owner1"),
      ownerGroups = Seq.empty,
      users = Seq("user1"),
      groups = Seq("group1")
    )

    val expected = Right(created)

    assert(actual.isRight)
    assert(actual == expected)
  }

  test("deleteUserRole deletes existing user role") {
    val rangerClient = mock[RangerClient]
    val gateway = new RangerRoleGateway(
      rangerClient
    )

    val role = RangerRole(
      id = 111,
      isEnabled = true,
      name = "prefix1_read",
      description = "",
      groups = Seq(RoleMember("group1", isAdmin = false)),
      users = Seq(
        RoleMember("user1", isAdmin = false),
        RoleMember("deployUser", isAdmin = true),
        RoleMember("owner1", isAdmin = true)
      ),
      roles = Seq.empty
    )

    (rangerClient.findRoleByName _)
      .expects("prefix1_read")
      .once()
      .returns(Right(Some(role)))

    (rangerClient.deleteRole _)
      .expects(role)
      .once()
      .returns(Right(()))

    val expected = Right(())

    val actual = gateway.deleteUserRole("prefix1")

    assert(actual.isRight)
    assert(actual == expected)
  }

  test("deleteUserRole returns no error if user role is not existing") {
    val rangerClient = mock[RangerClient]
    val gateway = new RangerRoleGateway(
      rangerClient
    )

    (rangerClient.findRoleByName _)
      .expects("prefix1_read")
      .once()
      .returns(Right(None))

    val expected = Right(())

    val actual = gateway.deleteUserRole("prefix1")

    assert(actual.isRight)
    assert(actual == expected)
  }

}
