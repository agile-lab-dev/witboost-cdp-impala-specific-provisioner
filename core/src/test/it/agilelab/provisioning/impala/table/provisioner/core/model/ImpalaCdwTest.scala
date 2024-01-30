package it.agilelab.provisioning.impala.table.provisioner.core.model

import io.circe.generic.auto.{ exportDecoder, exportEncoder }
import io.circe.syntax.EncoderOps
import io.circe.{ yaml, Json }
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaCdw.ImpalaProvisionRequestOps
import it.agilelab.provisioning.mesh.self.service.api.model.Component.{ DataContract, OutputPort }
import it.agilelab.provisioning.mesh.self.service.api.model.{ DataProduct, ProvisionRequest }
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
}
