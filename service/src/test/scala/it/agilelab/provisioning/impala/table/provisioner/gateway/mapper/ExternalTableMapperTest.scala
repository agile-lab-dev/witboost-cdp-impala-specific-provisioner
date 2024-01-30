package it.agilelab.provisioning.impala.table.provisioner.gateway.mapper

import cats.data.NonEmptyList
import io.circe.Json
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaDataType.{
  ImpalaInt,
  ImpalaString,
  ImpalaStruct,
  StructType
}
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaFormat.Csv
import it.agilelab.provisioning.impala.table.provisioner.core.model._
import it.agilelab.provisioning.mesh.self.service.api.model.Component.{ DataContract, OutputPort }
import it.agilelab.provisioning.mesh.self.service.api.model.DataProduct
import it.agilelab.provisioning.mesh.self.service.api.model.openmetadata.{
  Column,
  ColumnConstraint,
  ColumnDataType
}
import it.agilelab.provisioning.mesh.self.service.core.gateway.ComponentGatewayError
import org.scalatest.EitherValues._
import org.scalatest.funsuite.AnyFunSuite

class ExternalTableMapperTest extends AnyFunSuite {
  val dp: DataProduct[Json] = DataProduct[Json](
    id = "urn:dmb:dp:$DPDomain:$DPName:$DPMajorVersion",
    name = "name",
    domain = "domain",
    environment = "environment",
    version = "version",
    dataProductOwner = "dataProductOwner",
    devGroup = "devGroup",
    ownerGroup = "ownerGroup",
    specific = Json.obj(),
    components = Seq.empty
  )

  test("genExternalTable return Right(ExternalTable)") {
    val component: OutputPort[PublicImpalaCdw] = OutputPort[PublicImpalaCdw](
      id = "urn:dmb:cmp:$DPDomain:$DPName:$DPMajorVersion:$OutputPortName",
      name = "name",
      description = "description",
      version = "version",
      dataContract = DataContract(
        schema = Seq(
          getC("name", ColumnDataType.STRING),
          getC("surname", ColumnDataType.STRING),
          getC("age", ColumnDataType.INT),
          getC("sex", ColumnDataType.CHAR, dl = Some(1)),
          getC(
            "jobExperiences",
            ColumnDataType.ARRAY,
            adt = Some(ColumnDataType.STRUCT),
            ch = Some(
              Seq(
                getC("title", ColumnDataType.STRING),
                getC("description", ColumnDataType.STRING),
                getC("nYears", ColumnDataType.INT)
              ))
          )
        )
      ),
      specific = PublicImpalaCdw(
        databaseName = "databaseName",
        tableName = "tableName",
        cdpEnvironment = "cdpEnvironment",
        cdwVirtualWarehouse = "service",
        format = Csv,
        location = "location",
        partitions = Some(Seq("name", "surname"))
      )
    )
    val actual = ExternalTableMapper.map(component.dataContract.schema, component.specific)
    val expected = Right(
      ExternalTable(
        database = "databaseName",
        tableName = "tableName",
        schema = Seq(
          Field("age", ImpalaDataType.ImpalaInt, None),
          Field("sex", ImpalaDataType.ImpalaChar(1), None),
          Field(
            "jobExperiences",
            ImpalaDataType.ImpalaArray(
              ImpalaStruct(
                NonEmptyList.fromListUnsafe(
                  List(
                    StructType("title", ImpalaString, None),
                    StructType("description", ImpalaString, None),
                    StructType("nYears", ImpalaInt, None)
                  )))),
            None
          )
        ),
        partitions = Seq(Field("name", ImpalaString, None), Field("surname", ImpalaString, None)),
        location = "location",
        format = Csv
      ))

    assert(actual == expected)
  }

  test("genExternalTable return Left") {
    val component: OutputPort[PublicImpalaCdw] = OutputPort[PublicImpalaCdw](
      id = "urn:dmb:cmp:$DPDomain:$DPName:$DPMajorVersion:$OutputPortName",
      name = "name",
      description = "description",
      version = "version",
      dataContract = DataContract(
        schema = Seq(
          getC("name", ColumnDataType.STRING),
          getC("surname", ColumnDataType.STRING),
          getC("age", ColumnDataType.INT),
          getC("sex", ColumnDataType.CHAR, dl = Some(1)),
          getC("jobExperiences", ColumnDataType.ARRAY)
        )
      ),
      specific = PublicImpalaCdw(
        databaseName = "databaseName",
        tableName = "tableName",
        cdpEnvironment = "cdpEnvironment",
        cdwVirtualWarehouse = "service",
        format = Csv,
        location = "location",
        partitions = Some(Seq("name", "surname"))
      )
    )
    val actual = ExternalTableMapper.map(component.dataContract.schema, component.specific)

    assert(actual.isLeft)
    assert(actual.left.value.isInstanceOf[ComponentGatewayError])
    assert(actual.left.value.error == "arrayDataType must be specified for ARRAY data type")
  }

  private def getC(
      name: String,
      dt: ColumnDataType,
      adt: Option[ColumnDataType] = None,
      dl: Option[Int] = None,
      dtd: Option[String] = None,
      desc: Option[String] = None,
      fqdn: Option[String] = None,
      tags: Option[Seq[String]] = None,
      constraint: Option[ColumnConstraint] = None,
      op: Option[Int] = None,
      js: Option[String] = None,
      ch: Option[Seq[Column]] = None
  ) = Column(name, dt, adt, dl, dtd, desc, fqdn, tags, constraint, op, js, ch)
}
