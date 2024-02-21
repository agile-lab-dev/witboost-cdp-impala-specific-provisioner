package it.agilelab.provisioning.impala.table.provisioner.gateway.resource

import io.circe.Json
import io.circe.generic.auto.exportEncoder
import io.circe.syntax.EncoderOps
import it.agilelab.provisioning.commons.principalsmapping.{ CdpIamPrincipals, CdpIamUser }
import it.agilelab.provisioning.impala.table.provisioner.common.{
  OutputPortFaker,
  ProvisionRequestFaker,
  StorageAreaFaker
}
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaFormat.Csv
import it.agilelab.provisioning.impala.table.provisioner.core.model.{
  ExternalTable,
  Field,
  ImpalaCdpAcl,
  ImpalaDataType,
  ImpalaStorageAreaCdw,
  ImpalaTableResource,
  PolicyAttachment,
  PrivateImpalaCdw
}
import it.agilelab.provisioning.mesh.self.service.api.model.Component.Workload
import it.agilelab.provisioning.mesh.self.service.api.model.openmetadata.{ Column, ColumnDataType }
import it.agilelab.provisioning.mesh.self.service.core.gateway.{
  ComponentGateway,
  ComponentGatewayError
}
import it.agilelab.provisioning.mesh.self.service.core.model.ProvisionCommand
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

class ImpalaGatewayTest extends AnyFunSuite with MockFactory {

  val opGatewayMock = mock[ComponentGateway[Json, Json, ImpalaTableResource, CdpIamPrincipals]]
  val storageGatewayMock = mock[ComponentGateway[Json, Json, ImpalaTableResource, CdpIamPrincipals]]

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

  val tableResource = ImpalaTableResource(
    ExternalTable(
      "databaseName",
      "tableName",
      Seq(Field("id", ImpalaDataType.ImpalaInt, None)),
      Seq(Field("part1", ImpalaDataType.ImpalaString, None)),
      "loc",
      Csv
    ),
    ImpalaCdpAcl(
      Seq(
        PolicyAttachment("123", "xy"),
        PolicyAttachment("456", "ttt"),
        PolicyAttachment("789", "loc")
      ),
      Seq.empty[PolicyAttachment]
    )
  )

  test("create output port") {
    val opCommand: ProvisionCommand[Json, Json] = ProvisionCommand("id", opRequest)

    val impalaGateway = new ImpalaGateway(opGatewayMock, storageGatewayMock)

    (opGatewayMock.create _).expects(opCommand).returns(Right(tableResource))

    val expected = tableResource.asJson
    val actual = impalaGateway.create(opCommand)

    assert(actual == Right(expected))
  }

  test("destroy output port") {
    val opCommand: ProvisionCommand[Json, Json] = ProvisionCommand("id", opRequest)

    val impalaGateway = new ImpalaGateway(opGatewayMock, storageGatewayMock)

    (opGatewayMock.destroy _).expects(opCommand).returns(Right(tableResource))

    val expected = tableResource.asJson
    val actual = impalaGateway.destroy(opCommand)

    assert(actual == Right(expected))
  }

  test("updateAcl output port") {
    val opCommand: ProvisionCommand[Json, Json] = ProvisionCommand("id", opRequest)
    val refs: Set[CdpIamPrincipals] = Set(CdpIamUser("user.surname", "user.surname", ""))
    val impalaGateway = new ImpalaGateway(opGatewayMock, storageGatewayMock)

    (opGatewayMock.updateAcl _).expects(opCommand, refs).returns(Right(refs))

    val actual = impalaGateway.updateAcl(opCommand, refs)

    assert(actual == Right(refs))
  }

  test("create output port fails if underlying component gateway failed") {
    val opCommand: ProvisionCommand[Json, Json] = ProvisionCommand("id", opRequest)

    val impalaGateway = new ImpalaGateway(opGatewayMock, storageGatewayMock)

    val error = ComponentGatewayError("Error")
    (opGatewayMock.create _).expects(opCommand).returns(Left(error))

    val actual = impalaGateway.create(opCommand)

    assert(actual == Left(error))
  }

  test("destroy output port if underlying component gateway failed") {
    val opCommand: ProvisionCommand[Json, Json] = ProvisionCommand("id", opRequest)

    val impalaGateway = new ImpalaGateway(opGatewayMock, storageGatewayMock)

    val error = ComponentGatewayError("Error")
    (opGatewayMock.destroy _).expects(opCommand).returns(Left(error))

    val actual = impalaGateway.destroy(opCommand)

    assert(actual == Left(error))
  }

  test("updateAcl output port if underlying component gateway failed") {
    val opCommand: ProvisionCommand[Json, Json] = ProvisionCommand("id", opRequest)
    val refs: Set[CdpIamPrincipals] = Set(CdpIamUser("user.surname", "user.surname", ""))
    val impalaGateway = new ImpalaGateway(opGatewayMock, storageGatewayMock)

    val error = ComponentGatewayError("Error")
    (opGatewayMock.updateAcl _).expects(opCommand, refs).returns(Left(error))

    val actual = impalaGateway.updateAcl(opCommand, refs)

    assert(actual == Left(error))
  }

  // Storage Area

  test("create storage area") {
    val saCommand: ProvisionCommand[Json, Json] = ProvisionCommand("id", saRequest)

    val impalaGateway = new ImpalaGateway(opGatewayMock, storageGatewayMock)

    (storageGatewayMock.create _).expects(saCommand).returns(Right(tableResource))

    val expected = tableResource.asJson
    val actual = impalaGateway.create(saCommand)

    assert(actual == Right(expected))
  }

  test("destroy storage area") {
    val saCommand: ProvisionCommand[Json, Json] = ProvisionCommand("id", saRequest)

    val impalaGateway = new ImpalaGateway(opGatewayMock, storageGatewayMock)

    (storageGatewayMock.destroy _).expects(saCommand).returns(Right(tableResource))

    val expected = tableResource.asJson
    val actual = impalaGateway.destroy(saCommand)

    assert(actual == Right(expected))
  }

  test("updateAcl storage area") {
    val saCommand: ProvisionCommand[Json, Json] = ProvisionCommand("id", saRequest)
    val refs: Set[CdpIamPrincipals] = Set(CdpIamUser("user.surname", "user.surname", ""))
    val impalaGateway = new ImpalaGateway(opGatewayMock, storageGatewayMock)

    val error = ComponentGatewayError("storage components don't support update ACL tasks")
    val actual = impalaGateway.updateAcl(saCommand, refs)

    assert(actual == Left(error))
  }

  test("create storage area fails if underlying component gateway failed") {
    val saCommand: ProvisionCommand[Json, Json] = ProvisionCommand("id", saRequest)

    val impalaGateway = new ImpalaGateway(opGatewayMock, storageGatewayMock)

    val error = ComponentGatewayError("Error")
    (storageGatewayMock.create _).expects(saCommand).returns(Left(error))

    val actual = impalaGateway.create(saCommand)

    assert(actual == Left(error))
  }

  test("destroy storage area fails if underlying component gateway failed") {
    val saCommand: ProvisionCommand[Json, Json] = ProvisionCommand("id", saRequest)

    val impalaGateway = new ImpalaGateway(opGatewayMock, storageGatewayMock)

    val error = ComponentGatewayError("Error")
    (storageGatewayMock.destroy _).expects(saCommand).returns(Left(error))

    val actual = impalaGateway.destroy(saCommand)

    assert(actual == Left(error))
  }

  // Workload

  test("create workload fails as impala provisioner doesn't support workloads") {
    val workCommand: ProvisionCommand[Json, Json] = ProvisionCommand("id", workRequest)
    val impalaGateway = new ImpalaGateway(opGatewayMock, storageGatewayMock)

    val error =
      ComponentGatewayError("Received kind 'workload' which is not supported on this provisioner")

    val actual = impalaGateway.create(workCommand)

    assert(actual == Left(error))
  }

  test("destroy workload fails as impala provisioner doesn't support workloads") {
    val workCommand: ProvisionCommand[Json, Json] = ProvisionCommand("id", workRequest)
    val impalaGateway = new ImpalaGateway(opGatewayMock, storageGatewayMock)

    val error =
      ComponentGatewayError("Received kind 'workload' which is not supported on this provisioner")

    val actual = impalaGateway.destroy(workCommand)

    assert(actual == Left(error))
  }

  test("updateAcl workload fails as impala provisioner doesn't support workloads") {
    val saCommand: ProvisionCommand[Json, Json] = ProvisionCommand("id", workRequest)
    val refs: Set[CdpIamPrincipals] = Set(CdpIamUser("user.surname", "user.surname", ""))
    val impalaGateway = new ImpalaGateway(opGatewayMock, storageGatewayMock)

    val error = ComponentGatewayError("workload components don't support update ACL tasks")
    val actual = impalaGateway.updateAcl(saCommand, refs)

    assert(actual == Left(error))
  }

}
