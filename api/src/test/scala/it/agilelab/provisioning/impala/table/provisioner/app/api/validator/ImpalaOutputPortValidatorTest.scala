package it.agilelab.provisioning.impala.table.provisioner.app.api.validator

import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax.EncoderOps
import it.agilelab.provisioning.impala.table.provisioner.common.{
  OutputPortFaker,
  ProvisionRequestFaker,
  StorageAreaFaker
}
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaFormat.Csv
import it.agilelab.provisioning.impala.table.provisioner.core.model.{
  PrivateImpalaStorageAreaCdw,
  PrivateImpalaTableCdw,
  PrivateImpalaViewCdw,
  PublicImpalaTableCdw,
  TableParams
}
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

class ImpalaOutputPortValidatorTest extends AnyFunSuite with MockFactory {
  test("test a valid public descriptor") {
    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        OutputPortFaker(PublicImpalaTableCdw(
          databaseName = "domain_dp_name_0",
          tableName = "domain_dp_name_0_cmp_name_poc",
          cdpEnvironment = "cdpEnv",
          cdwVirtualWarehouse = "service",
          format = Csv,
          location = "s3a://bucket/path/",
          partitions = None,
          tableParams = Some(
            TableParams(
              header = Some(false),
              delimiter = Some(","),
              tblProperties = Map.empty
            )
          )
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

    val validator =
      ImpalaOutputPortValidator.outputPortImpalaCdwValidator(cdpValidator, locationValidator)
    val actual = validator.validate(request) match {
      case Right(value) => value.isValid
      case Left(_)      => false
    }
    assert(actual)
  }

  test("test an invalid public descriptor") {
    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        OutputPortFaker(PublicImpalaTableCdw(
          databaseName = "domain_dp_name_0",
          tableName = "domain_dp_name_0_cmp_name_poc",
          cdpEnvironment = "cdpEnv",
          cdwVirtualWarehouse = "service",
          format = Csv,
          location = "loc",
          partitions = None,
          tableParams = None
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

    val validator =
      ImpalaOutputPortValidator.outputPortImpalaCdwValidator(cdpValidator, locationValidator)
    val actual = validator.validate(request) match {
      case Right(value) => value.isInvalid
      case Left(_)      => false
    }
    assert(actual)
  }

  test("test an invalid public descriptor due to a wrong delimiter") {
    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        OutputPortFaker(PublicImpalaTableCdw(
          databaseName = "domain_dp_name_0",
          tableName = "domain_dp_name_0_cmp_name_poc",
          cdpEnvironment = "cdpEnv",
          cdwVirtualWarehouse = "service",
          format = Csv,
          location = "loc",
          partitions = None,
          tableParams = Some(
            TableParams(
              header = Some(false),
              delimiter = Some("DELIM"),
              tblProperties = Map.empty
            )
          )
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

    val validator =
      ImpalaOutputPortValidator.outputPortImpalaCdwValidator(cdpValidator, locationValidator)
    val actual = validator.validate(request) match {
      case Right(value) => value.isInvalid
      case Left(_)      => false
    }
    assert(actual)
  }

  test("test a valid private op table descriptor") {
    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        OutputPortFaker(PrivateImpalaTableCdw(
          databaseName = "domain_dp_name_0",
          tableName = "domain_dp_name_0_cmp_name_poc",
          format = Csv,
          location = "/hdfs/path",
          partitions = None,
          tableParams = Some(
            TableParams(
              header = Some(false),
              delimiter = Some(","),
              tblProperties = Map.empty
            )
          )
        ).asJson).build()
      )
      .build()

    val locationValidator = stub[LocationValidator]
    (locationValidator.isValidLocation _)
      .when("/hdfs/path")
      .returns(true)

    val validator = ImpalaOutputPortValidator.privateOutputPortImpalaCdwValidator(locationValidator)
    val actual = validator.validate(request) match {
      case Right(value) => value.isValid
      case Left(_)      => false
    }
    assert(actual)
  }

  test("test an invalid private op table descriptor") {
    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        OutputPortFaker(
          PrivateImpalaTableCdw(
            databaseName = "domain_dp_name_0",
            tableName = "domain_dp_name_0_cmp_name_poc",
            format = Csv,
            location = "loc",
            partitions = None,
            tableParams = None
          ).asJson).build()
      )
      .build()

    val locationValidator = stub[LocationValidator]
    (locationValidator.isValidLocation _)
      .when("loc")
      .returns(false)

    val validator = ImpalaOutputPortValidator.privateOutputPortImpalaCdwValidator(locationValidator)
    val actual = validator.validate(request) match {
      case Right(value) => value.isInvalid
      case Left(_)      => false
    }
    assert(actual)
  }

  test("test an invalid private op table descriptor due to a wrong delimiter") {
    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        OutputPortFaker(PrivateImpalaTableCdw(
          databaseName = "domain_dp_name_0",
          tableName = "domain_dp_name_0_cmp_name_poc",
          format = Csv,
          location = "loc",
          partitions = None,
          tableParams = Some(
            TableParams(
              header = Some(false),
              delimiter = Some("DELIM"),
              tblProperties = Map.empty
            )
          )
        ).asJson).build()
      )
      .build()

    val locationValidator = stub[LocationValidator]
    (locationValidator.isValidLocation _)
      .when("loc")
      .returns(true)

    val validator = ImpalaOutputPortValidator.privateOutputPortImpalaCdwValidator(locationValidator)
    val actual = validator.validate(request) match {
      case Right(value) => value.isInvalid
      case Left(_)      => false
    }
    assert(actual)
  }

  test("test a valid private op view descriptor") {
    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        OutputPortFaker(
          PrivateImpalaViewCdw(
            databaseName = "domain_dp_name_0",
            viewName = "domain_dp_name_0_cmp_name_view_poc",
            tableName = "domain_dp_name_0_cmp_name_poc"
          ).asJson)
          .withName("cmp-name-view")
          .withId("urn:dmb:cmp:domain:dp-name:0:cmp-name-view")
          .build()
      )
      .build()

    val locationValidator = stub[LocationValidator]

    val validator = ImpalaOutputPortValidator.privateOutputPortImpalaCdwValidator(locationValidator)
    val actual = validator.validate(request) match {
      case Right(value) => value.isValid
      case Left(_)      => false
    }
    assert(actual)
  }

  test("test a storage area descriptor fails as this is an output port validator") {
    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        StorageAreaFaker(PrivateImpalaStorageAreaCdw(
          databaseName = "domain_dp_name_0",
          tableName = "domain_dp_name_0_cmp_name_poc",
          format = Csv,
          location = "loc",
          partitions = None,
          tableSchema = Seq.empty,
          tableParams = None
        ).asJson).build()
      )
      .build()

    val locationValidator = stub[LocationValidator]
    (locationValidator.isValidLocation _)
      .when("loc")
      .returns(true)

    val validator = ImpalaOutputPortValidator.privateOutputPortImpalaCdwValidator(locationValidator)
    val actual = validator.validate(request) match {
      case Right(value) => value.isInvalid
      case Left(_)      => false
    }
    assert(actual)
  }
}
