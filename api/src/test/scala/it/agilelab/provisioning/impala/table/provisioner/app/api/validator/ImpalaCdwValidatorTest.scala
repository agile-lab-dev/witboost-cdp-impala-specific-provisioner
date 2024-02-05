package it.agilelab.provisioning.impala.table.provisioner.app.api.validator

import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax.EncoderOps
import it.agilelab.provisioning.impala.table.provisioner.common.{
  OutputPortFaker,
  ProvisionRequestFaker
}
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaFormat.Csv
import it.agilelab.provisioning.impala.table.provisioner.core.model.{
  PrivateImpalaCdw,
  PublicImpalaCdw
}
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

class ImpalaCdwValidatorTest extends AnyFunSuite with MockFactory {
  test("test a valid public descriptor") {
    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        OutputPortFaker(PublicImpalaCdw(
          databaseName = "domain_dp_name_0",
          tableName = "domain_dp_name_0_cmp_name_poc",
          cdpEnvironment = "cdpEnv",
          cdwVirtualWarehouse = "service",
          format = Csv,
          location = "s3a://bucket/path/",
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
    (locationValidator.isValidLocation _)
      .when("s3a://bucket/path/")
      .returns(true)

    (locationValidator.locationExists _)
      .when("s3a://bucket/path/")
      .returns(true)

    val validator = ImpalaCdwValidator.impalaCdwValidator(cdpValidator, locationValidator)
    val actual = validator.validate(request) match {
      case Right(value) => value.isValid
      case Left(_)      => false
    }
    assert(actual)
  }

  test("test a invalid public descriptor") {
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
      .returns(false)

    (cdpValidator.cdwVirtualClusterExists _)
      .when("cdpEnv", "service")
      .returns(true)

    val locationValidator = stub[LocationValidator]
    (locationValidator.locationExists _)
      .when("loc")
      .returns(true)

    val validator = ImpalaCdwValidator.impalaCdwValidator(cdpValidator, locationValidator)
    val actual = validator.validate(request) match {
      case Right(value) => value.isValid
      case Left(_)      => false
    }
    assert(!actual)
  }

  test("test a valid private descriptor") {
    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        OutputPortFaker(
          PrivateImpalaCdw(
            databaseName = "domain_dp_name_0",
            tableName = "domain_dp_name_0_cmp_name_poc",
            format = Csv,
            location = "/hdfs/path",
            partitions = None
          ).asJson).build()
      )
      .build()

    val locationValidator = stub[LocationValidator]
    (locationValidator.isValidLocation _)
      .when("/hdfs/path")
      .returns(true)

    val validator = ImpalaCdwValidator.privateImpalaCdwValidator(locationValidator)
    val actual = validator.validate(request) match {
      case Right(value) => value.isValid
      case Left(_)      => false
    }
    assert(actual)
  }

  test("test a invalid private descriptor") {
    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        OutputPortFaker(
          PrivateImpalaCdw(
            databaseName = "domain_dp_name_0",
            tableName = "domain_dp_name_0_cmp_name_poc",
            format = Csv,
            location = "loc",
            partitions = None
          ).asJson).build()
      )
      .build()

    val locationValidator = stub[LocationValidator]
    (locationValidator.isValidLocation _)
      .when("loc")
      .returns(false)

    val validator = ImpalaCdwValidator.privateImpalaCdwValidator(locationValidator)
    val actual = validator.validate(request) match {
      case Right(value) => value.isValid
      case Left(_)      => false
    }
    assert(!actual)
  }
}
