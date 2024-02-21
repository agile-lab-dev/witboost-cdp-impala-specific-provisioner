package it.agilelab.provisioning.impala.table.provisioner.app.api.validator

import cats.data.NonEmptyList
import cats.data.Validated.{ Invalid, Valid }
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax.EncoderOps
import it.agilelab.provisioning.commons.validator.{ ValidationFail, Validator }
import it.agilelab.provisioning.impala.table.provisioner.common.{
  OutputPortFaker,
  ProvisionRequestFaker,
  StorageAreaFaker
}
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaFormat.Csv
import it.agilelab.provisioning.impala.table.provisioner.core.model.{
  ImpalaStorageAreaCdw,
  PrivateImpalaCdw
}
import it.agilelab.provisioning.mesh.self.service.api.model.Component.{
  OutputPort,
  StorageArea,
  Workload
}
import it.agilelab.provisioning.mesh.self.service.api.model.openmetadata.{ Column, ColumnDataType }
import it.agilelab.provisioning.mesh.self.service.api.model.{ DataProduct, ProvisionRequest }
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

class ImpalaCdwValidatorTest extends AnyFunSuite with MockFactory {

  val opValidator = mock[Validator[ProvisionRequest[Json, Json]]]
  val saValidator = mock[Validator[ProvisionRequest[Json, Json]]]

  val opRequest = ProvisionRequestFaker[Json, Json](Json.obj())
    .withComponent(
      OutputPortFaker(
        PrivateImpalaCdw(
          databaseName = "domain_dp_name_0",
          tableName = "domain_dp_name_0_cmp_name_poc",
          format = Csv,
          location = "s3a://bucket/path/",
          partitions = None
        ).asJson).build()
    )
    .build()

  val saRequest = ProvisionRequestFaker[Json, Json](Json.obj())
    .withComponent(
      StorageAreaFaker(
        ImpalaStorageAreaCdw(
          databaseName = "domain_dp_name_0",
          tableName = "domain_dp_name_0_cmp_name_poc",
          format = Csv,
          location = "s3a://bucket/path/",
          partitions = None,
          tableSchema = Seq(
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
        ).asJson
      ).build()
    )
    .build()

  val workRequest = ProvisionRequestFaker[Json, Json](Json.obj())
    .withComponent(
      Workload(
        id = "id",
        name = "workload",
        description = "description",
        version = "0.0.0",
        specific = Json.obj()
      )
    )
    .build()

  test("validate output port") {
    val validator = new ImpalaCdwValidator(opValidator, saValidator)
    val expected = Right(Valid(opRequest))
    (opValidator.validate _).expects(opRequest).returns(expected)

    val actual = validator.validate(opRequest)
    assert(expected == actual)
  }

  test("validate storage area") {
    val validator = new ImpalaCdwValidator(opValidator, saValidator)
    val expected = Right(Valid(saRequest))

    (saValidator.validate _).expects(saRequest).returns(expected)

    val actual = validator.validate(saRequest)
    assert(expected == actual)
  }

  test("validate output port fails if underlying validator fails") {
    val validator = new ImpalaCdwValidator(opValidator, saValidator)
    val expected = Right(Invalid(NonEmptyList.one(ValidationFail(opRequest, "error"))))
    (opValidator.validate _).expects(opRequest).returns(expected)

    val actual = validator.validate(opRequest)
    assert(expected == actual)
  }

  test("validate storage area fails if underlying validator fails") {
    val validator = new ImpalaCdwValidator(opValidator, saValidator)
    val expected = Right(Invalid(NonEmptyList.one(ValidationFail(saRequest, "error"))))

    (saValidator.validate _).expects(saRequest).returns(expected)

    val actual = validator.validate(saRequest)
    assert(expected == actual)
  }

  test("validate workload fails as Impala provisioner doesn't handle workloads") {
    val validator = new ImpalaCdwValidator(opValidator, saValidator)
    val error = Right(
      Invalid(
        NonEmptyList.one(ValidationFail(
          workRequest,
          "Received kind 'workload' which is not supported by the provisioner"))))

    val actual = validator.validate(workRequest)
    assert(actual == error)
  }

  test("validate provision request without component fails") {
    val provRequest = ProvisionRequestFaker[Json, Json](Json.obj()).build()
    val validator = new ImpalaCdwValidator(opValidator, saValidator)
    val error = Right(
      Invalid(
        NonEmptyList.one(ValidationFail(
          provRequest,
          "Received provisioning request does not contain a component to deploy"))))
    val actual = validator.validate(provRequest)
    assert(actual == error)
  }

  test("withinOutputPortReq returns rule outcome if component is output port") {
    val trueRule = ImpalaCdwValidator.withinOutputPortReq[PrivateImpalaCdw](opRequest) {
      case (_, _) =>
        true
    }
    assert(trueRule)

    val falseRule = ImpalaCdwValidator.withinOutputPortReq[PrivateImpalaCdw](opRequest) {
      case (_, _) =>
        false
    }
    assert(!falseRule)
  }

  test("withinOutputPortReq returns error if component is not and output port") {
    val falseComponent = ImpalaCdwValidator.withinOutputPortReq[PrivateImpalaCdw](saRequest) {
      case (_, _) =>
        true
    }
    assert(!falseComponent)
  }

  test("withinStorageAreaReq returns rule outcome if component is storage area") {
    val trueRule = ImpalaCdwValidator.withinStorageAreaReq[ImpalaStorageAreaCdw](saRequest) {
      case (_, _) =>
        true
    }
    assert(trueRule)

    val falseRule = ImpalaCdwValidator.withinStorageAreaReq[ImpalaStorageAreaCdw](saRequest) {
      case (_, _) =>
        false
    }
    assert(!falseRule)
  }

  test("withinStorageAreaReq returns error if component is not and output port") {
    val falseComponent = ImpalaCdwValidator.withinStorageAreaReq[ImpalaStorageAreaCdw](opRequest) {
      case (_, _) => true
    }
    assert(!falseComponent)
  }

}
