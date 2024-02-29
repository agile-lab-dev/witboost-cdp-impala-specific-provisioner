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

  val baseComponent: OutputPort[PublicImpalaTableCdw] = OutputPort[PublicImpalaTableCdw](
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
    specific = PublicImpalaTableCdw(
      databaseName = "databaseName",
      tableName = "tableName",
      cdpEnvironment = "cdpEnvironment",
      cdwVirtualWarehouse = "service",
      format = Csv,
      location = "location",
      partitions = Some(Seq("name", "surname")),
      tableParams = None
    )
  )

  test("genExternalTable simple table return Right(ExternalTable)") {
    val actual = ExternalTableMapper.map(baseComponent.dataContract.schema, baseComponent.specific)
    val expected = Right(
      ExternalTable(
        database = "databaseName",
        name = "tableName",
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
        format = Csv,
        None,
        Map.empty,
        header = false
      ))

    assert(actual == expected)
  }

  test("genExternalTable simpel table return Left") {
    val component: OutputPort[PublicImpalaTableCdw] = baseComponent.copy(dataContract =
      DataContract(
        schema = Seq(
          getC("name", ColumnDataType.STRING),
          getC("surname", ColumnDataType.STRING),
          getC("age", ColumnDataType.INT),
          getC("sex", ColumnDataType.CHAR, dl = Some(1)),
          getC("jobExperiences", ColumnDataType.ARRAY)
        )
      ))

    val actual = ExternalTableMapper.map(component.dataContract.schema, component.specific)

    assert(actual.isLeft)
    assert(actual.left.value.isInstanceOf[ComponentGatewayError])
    assert(actual.left.value.error == "arrayDataType must be specified for ARRAY data type")
  }

  test("genExternalTable table with extra tableParams return Right(ExternalTable)") {
    val component: OutputPort[PublicImpalaTableCdw] = baseComponent.copy(
      specific = PublicImpalaTableCdw(
        databaseName = "databaseName",
        tableName = "tableName",
        cdpEnvironment = "cdpEnvironment",
        cdwVirtualWarehouse = "service",
        format = Csv,
        location = "location",
        partitions = Some(Seq("name", "surname")),
        tableParams = Some(
          TableParams(
            header = Some(true),
            delimiter = Some("0x2C"),
            tblProperties = Map(
              "property1" -> "value1",
              "property2" -> "value2"
            )
          )
        )
      )
    )
    val actual = ExternalTableMapper.map(component.dataContract.schema, component.specific)
    val expected = Right(
      ExternalTable(
        database = "databaseName",
        name = "tableName",
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
        format = Csv,
        Some(','.toByte),
        Map(
          "property1" -> "value1",
          "property2" -> "value2"
        ),
        header = true
      ))

    assert(actual == expected)
  }

  test(
    "genExternalTable table with extra tableParams but wrong delimiter return Left(ComponentGatewayError)") {
    val component: OutputPort[PublicImpalaTableCdw] = baseComponent.copy(
      specific = PublicImpalaTableCdw(
        databaseName = "databaseName",
        tableName = "tableName",
        cdpEnvironment = "cdpEnvironment",
        cdwVirtualWarehouse = "service",
        format = Csv,
        location = "location",
        partitions = Some(Seq("name", "surname")),
        tableParams = Some(
          TableParams(
            header = Some(true),
            delimiter = Some("MyFunnyDelim"),
            tblProperties = Map(
              "property1" -> "value1",
              "property2" -> "value2"
            )
          )
        )
      )
    )
    val actual = ExternalTableMapper.map(component.dataContract.schema, component.specific)
    println(actual.left.toOption.get.error)
    val expected = Left(ComponentGatewayError(
      "Failed to parse delimiter 'MyFunnyDelim', is not a hex number nor a single ASCII character"))

    assert(actual == expected)
  }

  test(
    "genExternalTable table with extra tableParams return Right(ExternalTable) with default values") {
    val component: OutputPort[PublicImpalaTableCdw] = baseComponent.copy(
      specific = PublicImpalaTableCdw(
        databaseName = "databaseName",
        tableName = "tableName",
        cdpEnvironment = "cdpEnvironment",
        cdwVirtualWarehouse = "service",
        format = ImpalaFormat.Textfile,
        location = "location",
        partitions = Some(Seq("name", "surname")),
        tableParams = Some(
          TableParams(
            header = None,
            delimiter = None,
            tblProperties = Map.empty
          )
        )
      )
    )
    val actual = ExternalTableMapper.map(component.dataContract.schema, component.specific)
    val expected = Right(
      ExternalTable(
        database = "databaseName",
        name = "tableName",
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
        format = ImpalaFormat.Textfile,
        None,
        Map.empty,
        header = false
      ))

    assert(actual == expected)
  }

  val stringToByteTests: Seq[(String, Option[Byte])] = Seq[(String, Option[Byte])](
    "0x2c" -> Some(44.toByte), // Hex value for ASCII comma
    ","    -> Some(44.toByte), // Single char ASCII comma
    "2c"   -> Some(44.toByte), // Hex value without 0x prefix
    "0xfe" -> Some(-2.toByte), // Hex value for Icelandic thorn, example taken from Impala docs
    "þ"    -> Some(-2.toByte), // Icelandic thorn, example taken from Impala docs
    "字"    -> None, // Character whose value is not in the range 0..256
    "DEL"  -> None // Not a single character
  )
  stringToByteTests.foreach { case (origin: String, byte: Option[Byte]) =>
    test(s"Converting hex value/single char '$origin' should convert to byte '$byte'") {
      val expected = ExternalTableMapper.hexStringToByte(origin)
      assert(expected == byte)
    }
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
