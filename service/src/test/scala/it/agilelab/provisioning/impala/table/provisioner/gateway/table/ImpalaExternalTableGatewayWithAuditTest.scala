package it.agilelab.provisioning.impala.table.provisioner.gateway.table

import it.agilelab.provisioning.commons.audit.Audit
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider.{
  ConnectionConfig,
  UsernamePasswordConnectionConfig
}
import it.agilelab.provisioning.impala.table.provisioner.core.model.{
  ExternalTable,
  ImpalaEntityResource,
  ImpalaFormat
}
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

class ImpalaExternalTableGatewayWithAuditTest extends AnyFunSuite with MockFactory {

  test("create with audit") {
    val gatewayMock = mock[ExternalTableGateway]
    val gatewayWithAudit = new ImpalaExternalTableGatewayWithAudit(
      gatewayMock,
      Audit.default("ImpalaExternalTableGateway"))

    val connectionConfig =
      UsernamePasswordConnectionConfig("host", "port", "schema", "user", "password", useSSL = true)
    val externalTable =
      ExternalTable(
        "database",
        "tableName",
        Seq.empty,
        Seq.empty,
        "location",
        ImpalaFormat.Csv,
        None,
        Map.empty,
        header = false)

    val entityResource = ImpalaEntityResource(externalTable, "jdbc://")

    (gatewayMock.create _)
      .expects(connectionConfig, externalTable, *)
      .returns(Right(entityResource))

    assert(
      gatewayWithAudit.create(connectionConfig, externalTable, ifNotExists = true) == Right(
        entityResource)
    )
  }

  test("drop with audit") {
    val gatewayMock = mock[ExternalTableGateway]
    val gatewayWithAudit = new ImpalaExternalTableGatewayWithAudit(
      gatewayMock,
      Audit.default("ImpalaExternalTableGateway"))

    val connectionConfig =
      UsernamePasswordConnectionConfig("host", "port", "schema", "user", "password", useSSL = true)
    val externalTable =
      ExternalTable(
        "database",
        "tableName",
        Seq.empty,
        Seq.empty,
        "location",
        ImpalaFormat.Csv,
        None,
        Map.empty,
        header = false)
    val entityResource = ImpalaEntityResource(externalTable, "jdbc://")

    (gatewayMock.drop _).expects(connectionConfig, externalTable, *).returns(Right(entityResource))

    assert(
      gatewayWithAudit.drop(connectionConfig, externalTable, ifExists = true) == Right(
        entityResource))
  }

}
