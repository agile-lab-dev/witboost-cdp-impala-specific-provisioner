package it.agilelab.provisioning.impala.table.provisioner.gateway.table

import it.agilelab.provisioning.commons.audit.Audit
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider.{
  ConnectionConfig,
  UsernamePasswordConnectionConfig
}
import it.agilelab.provisioning.impala.table.provisioner.core.model.{ ExternalTable, ImpalaFormat }
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
      ExternalTable("database", "tableName", Seq.empty, Seq.empty, "location", ImpalaFormat.Csv)

    (gatewayMock.create _).expects(connectionConfig, externalTable, *).returns(Right(()))

    assert(
      gatewayWithAudit.create(connectionConfig, externalTable, ifNotExists = true) == Right(())
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
      ExternalTable("database", "tableName", Seq.empty, Seq.empty, "location", ImpalaFormat.Csv)

    (gatewayMock.drop _).expects(connectionConfig, externalTable, *).returns(Right(()))

    assert(
      gatewayWithAudit.drop(connectionConfig, externalTable, ifExists = true) == Right(())
    )
  }

}
