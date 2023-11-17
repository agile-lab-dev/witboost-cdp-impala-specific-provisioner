package it.agilelab.provisioning.impala.table.provisioner.repository

import it.agilelab.provisioning.mesh.self.service.lambda.core.model.Role
import org.scalatest.funsuite.AnyFunSuite

class RoleConfigRepositoryTest extends AnyFunSuite {

  val roleConfigRepo = new RoleConfigRepository()

  test("load all roles from config") {
    val roles = roleConfigRepo.findAll(None)
    assert(roles.isRight)
    roles.foreach { roles =>
      // see src/test/resources/application.conf
      assert(roles.length == 2)
    }
  }

  test("load a specific role from config") {
    val expectedRole = Role(
      name = "platform-team-role",
      domain = "",
      iamRole = "",
      iamRoleArn = "",
      cdpRole = "cdp:role:test:platform-team-role",
      cdpRoleCrn = "")

    val maybeRole = roleConfigRepo.findById(expectedRole.name)
    assert(maybeRole.isRight)
    maybeRole.foreach { optionRole =>
      assert(optionRole.nonEmpty)
      optionRole.foreach { role =>
        assert(role.equals(expectedRole))
      }
    }
  }

  test("try to load an inexistent role from config") {
    val expectedRole = Role(
      name = "platform-team-role-inexistent",
      domain = "",
      iamRole = "",
      iamRoleArn = "",
      cdpRole = "cdp:role:test:platform-team-role",
      cdpRoleCrn = "")

    val maybeRole = roleConfigRepo.findById(expectedRole.name)
    assert(maybeRole.isRight)
    maybeRole.foreach { optionRole =>
      assert(optionRole.isEmpty)
    }
  }

}
