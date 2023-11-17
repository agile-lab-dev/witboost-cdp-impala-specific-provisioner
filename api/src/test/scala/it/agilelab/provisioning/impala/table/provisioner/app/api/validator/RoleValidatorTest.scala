package it.agilelab.provisioning.impala.table.provisioner.app.api.validator

import it.agilelab.provisioning.mesh.repository.Repository
import it.agilelab.provisioning.mesh.repository.RepositoryError.FindEntityByIdErr
import it.agilelab.provisioning.mesh.self.service.lambda.core.model.Role
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

class RoleValidatorTest extends AnyFunSuite with MockFactory {

  val repository: Repository[Role, String, Unit] = stub[Repository[Role, String, Unit]]
  val roleValidator = new RoleValidator(repository)

  test("validate return false if error") {
    (repository.findById _)
      .when(*)
      .returns(Left(FindEntityByIdErr("x", new IllegalArgumentException("xx"))))
    val actual = roleValidator.rolesExist(
      Seq("my-role-1", "my-role-2")
    )
    assert(!actual)
  }

  test("validate return false if all roles do not exist") {
    (repository.findById _)
      .when("my-role-1")
      .returns(Right(None))
    (repository.findById _)
      .when("my-role-2")
      .returns(Right(None))

    val actual = roleValidator.rolesExist(
      Seq("my-role-1", "my-role-2")
    )

    assert(!actual)
  }

  test("validate return false if one role does not exists") {
    (repository.findById _)
      .when("my-role-1")
      .returns(Right(Some(Role("a", "b", "c", "d", "e", "f"))))
    (repository.findById _)
      .when("my-role-2")
      .returns(Right(None))

    val actual = roleValidator.rolesExist(
      Seq("my-role-1", "my-role-2")
    )

    assert(!actual)
  }

  test("validate return true if all roles exist") {
    (repository.findById _)
      .when("my-role-1")
      .returns(Right(Some(Role("a", "b", "c", "d", "e", "f"))))
    (repository.findById _)
      .when("my-role-2")
      .returns(Right(Some(Role("g", "h", "i", "j", "k", "l"))))

    val actual = roleValidator.rolesExist(
      Seq("my-role-1", "my-role-2")
    )

    assert(actual)
  }

}
