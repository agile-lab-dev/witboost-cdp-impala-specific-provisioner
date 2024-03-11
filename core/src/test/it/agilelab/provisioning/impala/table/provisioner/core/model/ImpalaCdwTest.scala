package it.agilelab.provisioning.impala.table.provisioner.core.model

import io.circe.generic.auto.{ exportDecoder, exportEncoder }
import io.circe.syntax.EncoderOps
import io.circe.{ yaml, Json }
import it.agilelab.provisioning.impala.table.provisioner.core.model.ComponentDecodeError.DecodeErr
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaCdw.ImpalaProvisionRequestOps
import it.agilelab.provisioning.mesh.self.service.api.model.Component.{
  DataContract,
  OutputPort,
  StorageArea
}
import it.agilelab.provisioning.mesh.self.service.api.model.openmetadata.{ Column, ColumnDataType }
import it.agilelab.provisioning.mesh.self.service.api.model.{ DataProduct, ProvisionRequest }
import org.scalatest.funsuite.AnyFunSuite

class ImpalaCdwTest extends AnyFunSuite {

  test("decode output port table specific without tableParams") {
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

    val expected = PublicImpalaTableCdw(
      databaseName = "finance_salary_test_cdp_0",
      tableName = "finance_salary_test_cdp_0_impala_cdp_output_port_development",
      cdpEnvironment = "cdp-env",
      cdwVirtualWarehouse = "impala-test-vw",
      format = ImpalaFormat.Csv,
      location = "s3a://path/to/folder/",
      partitions = Some(List.empty),
      tableParams = None
    )
    val actual = specificJson.as[ImpalaCdw]

    assert(actual == Right(expected))
  }

  test("encode output port table specific without tableParams") {
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

    val specific: ImpalaCdw = PublicImpalaTableCdw(
      databaseName = "finance_salary_test_cdp_0",
      tableName = "finance_salary_test_cdp_0_impala_cdp_output_port_development",
      cdpEnvironment = "cdp-env",
      cdwVirtualWarehouse = "impala-test-vw",
      format = ImpalaFormat.Csv,
      location = "s3a://path/to/folder/",
      partitions = Some(List.empty),
      tableParams = None
    )
    val actual = specific.asJson.deepDropNullValues

    assert(actual == expected)
  }

  test("decode output port table specific with tableParams") {
    val specificJson = yaml.parser
      .parse("""
          | databaseName: finance_salary_test_cdp_0
          | tableName: finance_salary_test_cdp_0_impala_cdp_output_port_development
          | cdpEnvironment: cdp-env
          | cdwVirtualWarehouse: impala-test-vw
          | format: CSV
          | location: s3a://path/to/folder/
          | partitions: []
          | tableParams:
          |   header: true
          |   tblProperties:
          |     key1: value1
          |     key2: value2
          |""".stripMargin)
      .toOption
      .get

    val expected = PublicImpalaTableCdw(
      databaseName = "finance_salary_test_cdp_0",
      tableName = "finance_salary_test_cdp_0_impala_cdp_output_port_development",
      cdpEnvironment = "cdp-env",
      cdwVirtualWarehouse = "impala-test-vw",
      format = ImpalaFormat.Csv,
      location = "s3a://path/to/folder/",
      partitions = Some(List.empty),
      tableParams = Some(
        TableParams(
          header = Some(true),
          delimiter = None,
          tblProperties = Map("key1" -> "value1", "key2" -> "value2"))
      )
    )
    val actual = specificJson.as[ImpalaCdw]

    assert(actual == Right(expected))
  }

  test("encode output port table specific with tableParams") {
    val expected = yaml.parser
      .parse("""
          | databaseName: finance_salary_test_cdp_0
          | tableName: finance_salary_test_cdp_0_impala_cdp_output_port_development
          | cdpEnvironment: cdp-env
          | cdwVirtualWarehouse: impala-test-vw
          | format: CSV
          | location: s3a://path/to/folder/
          | partitions: []
          | tableParams:
          |   header: true
          |   tblProperties:
          |     key1: value1
          |     key2: value2
          |""".stripMargin)
      .toOption
      .get

    val specific: ImpalaCdw = PublicImpalaTableCdw(
      databaseName = "finance_salary_test_cdp_0",
      tableName = "finance_salary_test_cdp_0_impala_cdp_output_port_development",
      cdpEnvironment = "cdp-env",
      cdwVirtualWarehouse = "impala-test-vw",
      format = ImpalaFormat.Csv,
      location = "s3a://path/to/folder/",
      partitions = Some(List.empty),
      tableParams = Some(
        TableParams(
          header = Some(true),
          delimiter = None,
          tblProperties = Map("key1" -> "value1", "key2" -> "value2"))
      )
    )
    val actual = specific.asJson.deepDropNullValues

    assert(actual == expected)
  }

  test("decode private output port table specific") {
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

    val expected = PrivateImpalaTableCdw(
      databaseName = "finance_salary_test_cdp_0",
      tableName = "finance_salary_test_cdp_0_impala_cdp_output_port_development",
      format = ImpalaFormat.Csv,
      location = "s3a://path/to/folder/",
      partitions = Some(List.empty),
      tableParams = None
    )
    val actual = specificJson.as[ImpalaCdw]

    assert(actual == Right(expected))
  }

  test("encode private output port table specific") {
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

    val specific: ImpalaCdw = PrivateImpalaTableCdw(
      databaseName = "finance_salary_test_cdp_0",
      tableName = "finance_salary_test_cdp_0_impala_cdp_output_port_development",
      format = ImpalaFormat.Csv,
      location = "s3a://path/to/folder/",
      partitions = Some(List.empty),
      tableParams = None
    )
    val actual = specific.asJson.deepDropNullValues

    assert(actual == expected)
  }

  test("decode private output port view specific") {
    val specificJson = yaml.parser
      .parse("""
          | databaseName: finance_salary_test_cdp_0
          | tableName: finance_salary_test_cdp_0_impala_cdp_output_port_development
          | viewName: finance_salary_test_cdp_0_impala_cdp_op_view_development
          |""".stripMargin)
      .toOption
      .get

    val expected = PrivateImpalaViewCdw(
      databaseName = "finance_salary_test_cdp_0",
      tableName = "finance_salary_test_cdp_0_impala_cdp_output_port_development",
      viewName = "finance_salary_test_cdp_0_impala_cdp_op_view_development"
    )
    val actual = specificJson.as[ImpalaCdw]

    assert(actual == Right(expected))
  }

  test("encode private output port view specific") {
    val expected = yaml.parser
      .parse("""
          | databaseName: finance_salary_test_cdp_0
          | tableName: finance_salary_test_cdp_0_impala_cdp_output_port_development
          | viewName: finance_salary_test_cdp_0_impala_cdp_op_view_development
          |""".stripMargin)
      .toOption
      .get

    val specific: ImpalaCdw = PrivateImpalaViewCdw(
      databaseName = "finance_salary_test_cdp_0",
      tableName = "finance_salary_test_cdp_0_impala_cdp_output_port_development",
      viewName = "finance_salary_test_cdp_0_impala_cdp_op_view_development"
    )
    val actual = specific.asJson.deepDropNullValues

    assert(actual == expected)
  }

  test("test get output port request") {
    val specific = PublicImpalaTableCdw(
      databaseName = "finance_salary_test_cdp_0",
      tableName = "finance_salary_test_cdp_0_impala_cdp_output_port_development",
      cdpEnvironment = "cdp-env",
      cdwVirtualWarehouse = "impala-test-vw",
      format = ImpalaFormat.Csv,
      location = "s3a://path/to/folder/",
      partitions = Some(List.empty),
      tableParams = None
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
        dataProductOwner = "john.doe",
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
    val actual = request.getOutputPortRequest[PublicImpalaTableCdw]

    assert(actual == expected)
  }

  test("test get output port request fails if there is no component") {
    val request = ProvisionRequest[Json, Json](
      DataProduct(
        id = "id",
        name = "test_cdp",
        domain = "finance",
        environment = "development",
        version = "0.0.0",
        dataProductOwner = "john.doe",
        devGroup = "dev",
        ownerGroup = "dev",
        specific = Json.obj(),
        components = List.empty
      ),
      None
    )

    val expected = Left(DecodeErr("Received provisioning request does not contain a component"))
    val actual = request.getOutputPortRequest[PublicImpalaTableCdw]

    assert(actual == expected)
  }


  test("decode private storage area table specific") {
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

    val expected = PrivateImpalaStorageAreaCdw(
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
      ),
      tableParams = None
    )

    val actual = specificJson.as[ImpalaCdw]

    assert(actual == Right(expected))
  }

  test("encode private storage area table specific") {
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

    val specific: ImpalaCdw = PrivateImpalaStorageAreaCdw(
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
      ),
      tableParams = None
    )
    val actual = specific.asJson.deepDropNullValues

    assert(actual == expected)
  }

  test("decode private storage area view specific") {
    val specificJson = yaml.parser
      .parse("""
          | databaseName: finance_salary_test_cdp_0
          | viewName: finance_salary_test_cdp_0_sa_view
          | queryStatement: "SELECT * FROM db.table"
          |""".stripMargin)
      .toOption
      .get

    val expected = PrivateImpalaStorageAreaViewCdw(
      databaseName = "finance_salary_test_cdp_0",
      viewName = "finance_salary_test_cdp_0_sa_view",
      queryStatement = "SELECT * FROM db.table",
      tableSchema = None
    )

    val actual = specificJson.as[ImpalaCdw]

    assert(actual == Right(expected))
  }

  test("encode private storage area view specific") {
    val expected = yaml.parser
      .parse("""
          | databaseName: finance_salary_test_cdp_0
          | viewName: finance_salary_test_cdp_0_sa_view
          | queryStatement: "SELECT * FROM db.table"
          |""".stripMargin)
      .toOption
      .get

    val specific = PrivateImpalaStorageAreaViewCdw(
      databaseName = "finance_salary_test_cdp_0",
      viewName = "finance_salary_test_cdp_0_sa_view",
      queryStatement = "SELECT * FROM db.table",
      tableSchema = None
    )

    val actual = specific.asJson.deepDropNullValues

    assert(actual == expected)
  }

  test("test get storage area request") {
    val specific = PrivateImpalaStorageAreaCdw(
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
      ),
      tableParams = None
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
        dataProductOwner = "john.doe",
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
    val actual = request.getStorageAreaRequest[PrivateImpalaStorageAreaCdw]

    assert(actual == expected)
  }

  test("test get storage area request fails if there is no component") {
    val request = ProvisionRequest[Json, Json](
      DataProduct(
        id = "id",
        name = "test_cdp",
        domain = "finance",
        environment = "development",
        version = "0.0.0",
        dataProductOwner = "john.doe",
        devGroup = "dev",
        ownerGroup = "dev",
        specific = Json.obj(),
        components = List.empty
      ),
      None
    )

    val expected = Left(DecodeErr("Received provisioning request does not contain a component"))
    val actual = request.getStorageAreaRequest[PublicImpalaTableCdw]

    assert(actual == expected)
  }

  test("test get output port request fails on wrong component type") {
    val specific = PrivateImpalaStorageAreaCdw(
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
      ),
      tableParams = None
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
        dataProductOwner = "john.doe",
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
    val actual = request.getOutputPortRequest[PrivateImpalaStorageAreaCdw]

    assert(actual == expected)
  }
}
