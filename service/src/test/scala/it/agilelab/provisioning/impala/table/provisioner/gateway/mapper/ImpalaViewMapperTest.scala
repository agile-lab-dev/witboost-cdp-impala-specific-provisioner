package it.agilelab.provisioning.impala.table.provisioner.gateway.mapper

import cats.data.NonEmptyList
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaDataType.{
  ImpalaInt,
  ImpalaString,
  ImpalaStruct,
  StructType
}
import it.agilelab.provisioning.impala.table.provisioner.core.model.{
  Field,
  ImpalaDataType,
  ImpalaView,
  PrivateImpalaViewCdw
}
import it.agilelab.provisioning.mesh.self.service.api.model.Component.{ DataContract, OutputPort }
import it.agilelab.provisioning.mesh.self.service.api.model.openmetadata.{
  Column,
  ColumnConstraint,
  ColumnDataType
}
import it.agilelab.provisioning.mesh.self.service.core.gateway.ComponentGatewayError
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.EitherValues._

class ImpalaViewMapperTest extends AnyFunSuite {

  test("map correct ImpalaView returns Right(ImpalaView)") {
    val component: OutputPort[PrivateImpalaViewCdw] = OutputPort[PrivateImpalaViewCdw](
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
      specific = PrivateImpalaViewCdw(
        databaseName = "databaseName",
        tableName = "tableName",
        viewName = "viewName"
      )
    )
    val actual = ImpalaViewMapper.map(component.dataContract.schema, component.specific)
    val expected = Right(
      ImpalaView(
        database = "databaseName",
        name = "viewName",
        schema = Seq(
          Field("name", ImpalaString, None),
          Field("surname", ImpalaString, None),
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
        readsFromTableName = "tableName"
      ))

    assert(actual == expected)
  }

  test("map incorrect ImpalaView returns return Left") {
    val component: OutputPort[PrivateImpalaViewCdw] = OutputPort[PrivateImpalaViewCdw](
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
      specific = PrivateImpalaViewCdw(
        databaseName = "databaseName",
        tableName = "tableName",
        viewName = "viewName"
      )
    )
    val actual = ImpalaViewMapper.map(component.dataContract.schema, component.specific)

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
