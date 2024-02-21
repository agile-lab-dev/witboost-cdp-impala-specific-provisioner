package it.agilelab.provisioning.impala.table.provisioner.core.model

import io.circe.generic.auto.{exportDecoder, exportEncoder}
import io.circe.syntax.EncoderOps
import io.circe.{Json, yaml}
import it.agilelab.provisioning.impala.table.provisioner.core.model.ComponentDecodeError.DecodeErr
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaCdw.ImpalaProvisionRequestOps
import it.agilelab.provisioning.mesh.self.service.api.model.Component.{DataContract, OutputPort, StorageArea}
import it.agilelab.provisioning.mesh.self.service.api.model.openmetadata.{Column, ColumnDataType}
import it.agilelab.provisioning.mesh.self.service.api.model.{DataProduct, ProvisionRequest}
import org.scalatest.funsuite.AnyFunSuite

class ImpalaCdwTest extends AnyFunSuite {

  test("decode output port specific") {
    val specificJson = yaml.parser
      .parse("""
        | databaseName: finance_salary_test_cdp_0
        | tableName: finance_salary_test_cdp_0_impala_cdp_output_port_development
        | cdpEnvironment: cdp-env
        | cdwVirtualWarehouse: impala-test-vw
        | format: CSV
        | location: s3a://path/to/folder/
        | partitions: []
        |""".stripMargin)
      .toOption
      .get

    val expected = PublicImpalaCdw(
      databaseName = "finance_salary_test_cdp_0",
      tableName = "finance_salary_test_cdp_0_impala_cdp_output_port_development",
      cdpEnvironment = "cdp-env",
      cdwVirtualWarehouse = "impala-test-vw",
      format = ImpalaFormat.Csv,
      location = "s3a://path/to/folder/",
      partitions = Some(List.empty)
    )
    val actual = specificJson.as[ImpalaCdw]

    assert(actual == Right(expected))
  }

  test("encode output port specific") {
    val expected = yaml.parser
      .parse("""
          | databaseName: finance_salary_test_cdp_0
          | tableName: finance_salary_test_cdp_0_impala_cdp_output_port_development
          | cdpEnvironment: cdp-env
          | cdwVirtualWarehouse: impala-test-vw
          | format: CSV
          | location: s3a://path/to/folder/
          | partitions: []
          |""".stripMargin)
      .toOption
      .get

    val specific: ImpalaCdw = PublicImpalaCdw(
      databaseName = "finance_salary_test_cdp_0",
      tableName = "finance_salary_test_cdp_0_impala_cdp_output_port_development",
      cdpEnvironment = "cdp-env",
      cdwVirtualWarehouse = "impala-test-vw",
      format = ImpalaFormat.Csv,
      location = "s3a://path/to/folder/",
      partitions = Some(List.empty)
    )
    val actual = specific.asJson

    assert(actual == expected)
  }

  test("decode private output port specific") {
    val specificJson = yaml.parser
      .parse("""
          | databaseName: finance_salary_test_cdp_0
          | tableName: finance_salary_test_cdp_0_impala_cdp_output_port_development
          | format: CSV
          | location: s3a://path/to/folder/
          | partitions: []
          |""".stripMargin)
      .toOption
      .get

    val expected = PrivateImpalaCdw(
      databaseName = "finance_salary_test_cdp_0",
      tableName = "finance_salary_test_cdp_0_impala_cdp_output_port_development",
      format = ImpalaFormat.Csv,
      location = "s3a://path/to/folder/",
      partitions = Some(List.empty)
    )
    val actual = specificJson.as[ImpalaCdw]

    assert(actual == Right(expected))
  }

  test("encode private output port specific") {
    val expected = yaml.parser
      .parse("""
          | databaseName: finance_salary_test_cdp_0
          | tableName: finance_salary_test_cdp_0_impala_cdp_output_port_development
          | format: CSV
          | location: s3a://path/to/folder/
          | partitions: []
          |""".stripMargin)
      .toOption
      .get

    val specific: ImpalaCdw = PrivateImpalaCdw(
      databaseName = "finance_salary_test_cdp_0",
      tableName = "finance_salary_test_cdp_0_impala_cdp_output_port_development",
      format = ImpalaFormat.Csv,
      location = "s3a://path/to/folder/",
      partitions = Some(List.empty)
    )
    val actual = specific.asJson

    assert(actual == expected)
  }

  test("test get output port request") {
    val specific = PublicImpalaCdw(
      databaseName = "finance_salary_test_cdp_0",
      tableName = "finance_salary_test_cdp_0_impala_cdp_output_port_development",
      cdpEnvironment = "cdp-env",
      cdwVirtualWarehouse = "impala-test-vw",
      format = ImpalaFormat.Csv,
      location = "s3a://path/to/folder/",
      partitions = Some(List.empty)
    )

    val op = OutputPort(
      id = "comp_id",
      name = "comp_name",
      description = "description",
      version = "0.0.0",
      dataContract = DataContract(Seq.empty),
      specific = specific.asJson
    )

    val request = ProvisionRequest[Json, Json](
      DataProduct(
        id = "id",
        name = "test_cdp",
        domain = "finance",
        environment = "development",
        version = "0.0.0",
        dataProductOwner = "sergio.mejia",
        devGroup = "dev",
        ownerGroup = "dev",
        specific = Json.obj(),
        components = List(
          op.asJson
        )
      ),
      Some(op)
    )

    val expected = Right(op.copy(specific = specific))
    val actual = request.getOutputPortRequest[PublicImpalaCdw]

    assert(actual == expected)
  }

  test("decode private storage area specific") {
    val specificJson = yaml.parser
      .parse("""
          | databaseName: finance_salary_test_cdp_0
          | tableName: finance_salary_test_cdp_0_impala_cdp_output_port_development
          | format: CSV
          | location: s3a://path/to/folder/
          | partitions: []
          | tableSchema:
          |   - name: Age
          |     dataType: DOUBLE
          |   - name: Gender
          |     dataType: STRING
          |   - name: Education_Level
          |     dataType: INT
          |""".stripMargin)
      .toOption
      .get

    val expected = ImpalaStorageAreaCdw(
      databaseName = "finance_salary_test_cdp_0",
      tableName = "finance_salary_test_cdp_0_impala_cdp_output_port_development",
      format = ImpalaFormat.Csv,
      location = "s3a://path/to/folder/",
      partitions = Some(List.empty),
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
    )

    val actual = specificJson.as[ImpalaCdw]

    assert(actual == Right(expected))
  }

  test("encode private storage area specific") {
    val expected = yaml.parser
      .parse("""
          | databaseName: finance_salary_test_cdp_0
          | tableName: finance_salary_test_cdp_0_impala_cdp_output_port_development
          | format: CSV
          | location: s3a://path/to/folder/
          | partitions: []
          | tableSchema:
          |   - name: Age
          |     dataType: DOUBLE
          |   - name: Gender
          |     dataType: STRING
          |   - name: Education_Level
          |     dataType: INT
          |""".stripMargin)
      .toOption
      .get

    val specific: ImpalaCdw = ImpalaStorageAreaCdw(
      databaseName = "finance_salary_test_cdp_0",
      tableName = "finance_salary_test_cdp_0_impala_cdp_output_port_development",
      format = ImpalaFormat.Csv,
      location = "s3a://path/to/folder/",
      partitions = Some(List.empty),
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
    )
    val actual = specific.asJson.deepDropNullValues

    assert(actual == expected)
  }

  test("test get storage area request") {
    val specific = ImpalaStorageAreaCdw(
      databaseName = "finance_salary_test_cdp_0",
      tableName = "finance_salary_test_cdp_0_impala_cdp_output_port_development",
      format = ImpalaFormat.Csv,
      location = "s3a://path/to/folder/",
      partitions = Some(List.empty),
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
    )

    val op = StorageArea(
      id = "comp_id",
      name = "comp_name",
      description = "description",
      owners = Seq.empty,
      specific = specific.asJson
    )

    val request = ProvisionRequest[Json, Json](
      DataProduct(
        id = "id",
        name = "test_cdp",
        domain = "finance",
        environment = "development",
        version = "0.0.0",
        dataProductOwner = "sergio.mejia",
        devGroup = "dev",
        ownerGroup = "dev",
        specific = Json.obj(),
        components = List(
          op.asJson
        )
      ),
      Some(op)
    )

    val expected = Right(op.copy(specific = specific))
    val actual = request.getStorageAreaRequest[ImpalaStorageAreaCdw]

    assert(actual == expected)
  }

  test("test get output port request fails on wrong component type") {
    val specific = ImpalaStorageAreaCdw(
      databaseName = "finance_salary_test_cdp_0",
      tableName = "finance_salary_test_cdp_0_impala_cdp_output_port_development",
      format = ImpalaFormat.Csv,
      location = "s3a://path/to/folder/",
      partitions = Some(List.empty),
      tableSchema = Seq(
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
    )

    val op = StorageArea(
      id = "comp_id",
      name = "comp_name",
      description = "description",
      owners = Seq.empty,
      specific = specific.asJson
    )

    val request = ProvisionRequest[Json, Json](
      DataProduct(
        id = "id",
        name = "test_cdp",
        domain = "finance",
        environment = "development",
        version = "0.0.0",
        dataProductOwner = "sergio.mejia",
        devGroup = "dev",
        ownerGroup = "dev",
        specific = Json.obj(),
        components = List(
          op.asJson
        )
      ),
      Some(op)
    )

    val expected = Left(DecodeErr("The provided component is not accepted by this provisioner"))
    val actual = request.getOutputPortRequest[ImpalaStorageAreaCdw]

    assert(actual == expected)
  }

}
