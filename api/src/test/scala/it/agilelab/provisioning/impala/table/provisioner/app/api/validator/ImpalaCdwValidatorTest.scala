package it.agilelab.provisioning.impala.table.provisioner.app.api.validator

import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax.EncoderOps
import it.agilelab.provisioning.impala.table.provisioner.common.{
  OutputPortFaker,
  ProvisionRequestFaker
}
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaFormat.Csv
import it.agilelab.provisioning.impala.table.provisioner.core.model.PublicImpalaCdw
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

class ImpalaCdwValidatorTest extends AnyFunSuite with MockFactory {
  test("test a valid descriptor") {
    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        OutputPortFaker(PublicImpalaCdw(
          databaseName = "domain_dp_name_0",
          tableName = "domain_dp_name_0_cmp_name_poc",
          cdpEnvironment = "cdpEnv",
          cdwVirtualWarehouse = "service",
          format = Csv,
          location = "loc",
          partitions = None
        ).asJson).build()
      )
      .build()

    val cdpValidator = stub[CdpValidator]
    (cdpValidator.cdpEnvironmentExists _)
      .when("cdpEnv")
      .returns(true)

    (cdpValidator.cdwVirtualClusterExists _)
      .when("cdpEnv", "service")
      .returns(true)

    val locationValidator = stub[LocationValidator]
    (locationValidator.locationExists _)
      .when("loc")
      .returns(true)

    val validator = ImpalaCdwValidator.impalaCdwValidator(cdpValidator, locationValidator)
    println(validator.validate(request))
    val actual = validator.validate(request) match {
      case Right(value) => value.isValid
      case Left(_)      => false
    }
    assert(actual)
  }

  test("test a invalid descriptor") {
    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        OutputPortFaker(PublicImpalaCdw(
          databaseName = "unformatted data base name",
          tableName = "domain_dp_name_0_cmp_name_poc",
          cdpEnvironment = "cdpEnv",
          cdwVirtualWarehouse = "service",
          format = Csv,
          location = "loc",
          partitions = None
        ).asJson).build()
      )
      .build()

    val cdpValidator = stub[CdpValidator]
    (cdpValidator.cdpEnvironmentExists _)
      .when("cdpEnv")
      .returns(true)

    (cdpValidator.cdwVirtualClusterExists _)
      .when("cdpEnv", "service")
      .returns(true)

    val locationValidator = stub[LocationValidator]
    (locationValidator.locationExists _)
      .when("loc")
      .returns(true)

    val validator = ImpalaCdwValidator.impalaCdwValidator(cdpValidator, locationValidator)
    println(validator.validate(request))
    val actual = validator.validate(request) match {
      case Right(value) => value.isValid
      case Left(_)      => false
    }
    assert(!actual)
  }
}
