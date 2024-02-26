package it.agilelab.provisioning.impala.table.provisioner.app.api.validator

import io.circe.Json
import io.circe.syntax.EncoderOps
import it.agilelab.provisioning.impala.table.provisioner.common.{
  OutputPortFaker,
  ProvisionRequestFaker,
  StorageAreaFaker,
  StorageAreaFakerBuilder
}
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaFormat.Csv
import it.agilelab.provisioning.impala.table.provisioner.core.model.{
  PrivateImpalaStorageAreaCdw,
  PrivateImpalaTableCdw,
  PublicImpalaTableCdw
}
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite
import io.circe.generic.auto._
import it.agilelab.provisioning.mesh.self.service.api.model.openmetadata.{ Column, ColumnDataType }

class ImpalaStorageAreaValidatorTest extends AnyFunSuite with MockFactory {

  val validTableSchema: Seq[Column] = Seq(
    Column(
      name = "Age",
      dataType = ColumnDataType.DOUBLE,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None
    ),
    Column(
      name = "Gender",
      dataType = ColumnDataType.STRING,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None
    ),
    Column(
      name = "Education_Level",
      dataType = ColumnDataType.INT,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None
    )
  )

  test("test a public descriptor") {
    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        StorageAreaFaker(PrivateImpalaStorageAreaCdw(
          databaseName = "domain_dp_name_0",
          tableName = "domain_dp_name_0_cmp_name_poc",
          format = Csv,
          location = "s3a://bucket/path/",
          partitions = None,
          tableSchema = Seq.empty
        ).asJson).build()
      )
      .build()

    val cdpValidator = stub[CdpValidator]
    val locationValidator = stub[LocationValidator]

    val validator =
      ImpalaStorageAreaValidator.storageAreaImpalaCdwValidator(cdpValidator, locationValidator)
    val actual = validator.validate(request) match {
      case Right(value) => value.isInvalid
      case Left(_)      => false
    }
    assert(actual)
  }

  test("test a valid private descriptor") {
    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        StorageAreaFaker(PrivateImpalaStorageAreaCdw(
          databaseName = "domain_dp_name_0",
          tableName = "domain_dp_name_0_cmp_name_poc",
          format = Csv,
          location = "/hdfs/path",
          partitions = None,
          tableSchema = validTableSchema
        ).asJson).build()
      )
      .build()

    val locationValidator = stub[LocationValidator]
    (locationValidator.isValidLocation _)
      .when("/hdfs/path")
      .returns(true)

    val validator =
      ImpalaStorageAreaValidator.privateStorageAreaImpalaCdwValidator(locationValidator)
    val actual = validator.validate(request) match {
      case Right(value) => value.isValid
      case Left(_)      => false
    }
    assert(actual)
  }

  test("test a invalid private descriptor") {
    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        StorageAreaFaker(
          PrivateImpalaStorageAreaCdw(
            databaseName = "domain_dp_name_0",
            tableName = "domain_dp_name_0_cmp_name_poc",
            format = Csv,
            location = "loc",
            partitions = None,
            tableSchema = validTableSchema
          ).asJson).build()
      )
      .build()

    val locationValidator = stub[LocationValidator]
    (locationValidator.isValidLocation _)
      .when("loc")
      .returns(false)

    val validator =
      ImpalaStorageAreaValidator.privateStorageAreaImpalaCdwValidator(locationValidator)
    val actual = validator.validate(request) match {
      case Right(value) => value.isInvalid
      case Left(_)      => false
    }
    assert(actual)

  }

  test("test a invalid private descriptor which sets inexistent partitions") {
    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        StorageAreaFaker(PrivateImpalaStorageAreaCdw(
          databaseName = "domain_dp_name_0",
          tableName = "domain_dp_name_0_cmp_name_poc",
          format = Csv,
          location = "loc",
          partitions = Some(Seq("inexistentColumn")),
          tableSchema = validTableSchema
        ).asJson).build()
      )
      .build()

    val locationValidator = stub[LocationValidator]
    (locationValidator.isValidLocation _)
      .when("loc")
      .returns(true)

    val validator =
      ImpalaStorageAreaValidator.privateStorageAreaImpalaCdwValidator(locationValidator)
    val actual = validator.validate(request) match {
      case Right(value) => value.isInvalid
      case Left(_)      => false
    }
    assert(actual)
  }

  test("test an output port descriptor fails as this is an storage area validator") {
    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        OutputPortFaker(
          PrivateImpalaTableCdw(
            databaseName = "domain_dp_name_0",
            tableName = "domain_dp_name_0_cmp_name_poc",
            format = Csv,
            location = "s3a://bucket/path/",
            partitions = None
          ).asJson).build()
      )
      .build()

    val locationValidator = stub[LocationValidator]
    (locationValidator.isValidLocation _)
      .when("s3a://bucket/path/")
      .returns(true)

    (locationValidator.locationExists _)
      .when("s3a://bucket/path/")
      .returns(true)

    val validator =
      ImpalaStorageAreaValidator.privateStorageAreaImpalaCdwValidator(locationValidator)
    val actual = validator.validate(request) match {
      case Right(value) => value.isInvalid
      case Left(_)      => false
    }
    assert(actual)
  }

}
