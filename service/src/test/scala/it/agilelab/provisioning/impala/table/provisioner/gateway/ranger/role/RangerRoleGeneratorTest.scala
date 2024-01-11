package it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.role

import it.agilelab.provisioning.commons.client.ranger.model.{ RangerRole, RoleMember }
import org.scalatest.funsuite.AnyFunSuite

class RangerRoleGeneratorTest extends AnyFunSuite {

  test("generateOwnerRoleName") {
    val actual = RangerRoleGenerator.generateOwnerRoleName("prefix1")
    val expected = "prefix1_owner"

    assert(actual == expected)
  }

  test("generateOwnerRoleName with cleaned chars") {
    val actual = RangerRoleGenerator.generateOwnerRoleName("prefix1!#")
    val expected = "prefix1___owner"

    assert(actual == expected)
  }

  test("generateUserRoleName") {
    val actual = RangerRoleGenerator.generateUserRoleName("prefix1")
    val expected = "prefix1_read"

    assert(actual == expected)
  }

  test("generateUserRoleName with cleaned chars") {
    val actual = RangerRoleGenerator.generateUserRoleName("prefix1-)")
    val expected = "prefix1___read"

    assert(actual == expected)
  }

  test("owner role creation") {
    val actual = RangerRoleGenerator.role(
      RangerRoleGenerator.generateOwnerRoleName("prefix1"),
      Seq("user1"),
      Seq("owner1"),
      Seq("group1"),
      Seq.empty,
      Seq.empty
    )

    val expected = RangerRole(
      id = 0,
      isEnabled = true,
      name = "prefix1_owner",
      description = "",
      groups = Seq(RoleMember("group1", isAdmin = false)),
      users = Seq(RoleMember("user1", isAdmin = false), RoleMember("owner1", isAdmin = true)),
      roles = Seq.empty
    )

    assert(actual == expected)
  }

  test("user role creation") {
    val actual = RangerRoleGenerator.role(
      RangerRoleGenerator.generateUserRoleName("prefix1"),
      Seq("user1"),
      Seq("owner1"),
      Seq("group1"),
      Seq.empty,
      Seq.empty
    )

    val expected = RangerRole(
      id = 0,
      isEnabled = true,
      name = "prefix1_read",
      description = "",
      groups = Seq(RoleMember("group1", isAdmin = false)),
      users = Seq(RoleMember("user1", isAdmin = false), RoleMember("owner1", isAdmin = true)),
      roles = Seq.empty
    )

    assert(actual == expected)
  }

}
