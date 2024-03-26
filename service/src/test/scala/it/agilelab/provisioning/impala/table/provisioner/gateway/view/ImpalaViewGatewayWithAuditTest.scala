package it.agilelab.provisioning.impala.table.provisioner.gateway.view

import it.agilelab.provisioning.commons.audit.Audit
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider.UsernamePasswordConnectionConfig
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.query.SqlGatewayError.ExecuteDDLErr
import it.agilelab.provisioning.impala.table.provisioner.core.model.{
  ImpalaEntityImpl,
  ImpalaEntityResource,
  ImpalaView
}
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

import java.sql.SQLException

class ImpalaViewGatewayWithAuditTest extends AnyFunSuite with MockFactory {

  val audit: Audit = mock[Audit]

  test("create with audit logs success info") {
    val gatewayMock = mock[ViewGateway]
    val gatewayWithAudit =
      new ImpalaViewGatewayWithAudit(gatewayMock, audit)

    inSequence(
      (audit.info _).expects(
        "Executing CreateImpalaView(ImpalaView(database,viewName,List(),Some(ImpalaEntityImpl(db,table,List())),None))"),
      (audit.info _).expects(
        "CreateImpalaView(ImpalaView(database,viewName,List(),Some(ImpalaEntityImpl(db,table,List())),None)) completed successfully")
    )

    val connectionConfig =
      UsernamePasswordConnectionConfig("host", "port", "schema", "user", "password", useSSL = true)
    val impalaView = ImpalaView(
      "database",
      "viewName",
      Seq.empty,
      readsFromSource = Some(ImpalaEntityImpl("db", "table", Seq.empty)),
      None)

    val entityResource = ImpalaEntityResource(impalaView, "jdbc://")

    (gatewayMock.create _).expects(connectionConfig, impalaView, *).returns(Right(entityResource))

    assert(
      gatewayWithAudit.create(connectionConfig, impalaView, ifNotExists = true) == Right(
        entityResource)
    )
  }

  test("create with audit logs error info") {
    val gatewayMock = mock[ViewGateway]
    val gatewayWithAudit =
      new ImpalaViewGatewayWithAudit(gatewayMock, audit)

    inSequence(
      (audit.info _).expects(
        "Executing CreateImpalaView(ImpalaView(database,viewName,List(),Some(ImpalaEntityImpl(db,table,List())),None))"),
      (audit.error _).expects(where { s: String =>
        s.startsWith(
          "CreateImpalaView(ImpalaView(database,viewName,List(),Some(ImpalaEntityImpl(db,table,List())),None)) failed. Details: ExecuteDDLErr(")
      })
    )

    val connectionConfig =
      UsernamePasswordConnectionConfig("host", "port", "schema", "user", "password", useSSL = true)
    val impalaView = ImpalaView(
      "database",
      "viewName",
      Seq.empty,
      readsFromSource = Some(ImpalaEntityImpl("db", "table", Seq.empty)),
      None)

    val error = new SQLException("Error")
    (gatewayMock.create _)
      .expects(connectionConfig, impalaView, *)
      .returns(Left(ExecuteDDLErr(error)))

    assert(
      gatewayWithAudit.create(connectionConfig, impalaView, ifNotExists = true) ==
        Left(ExecuteDDLErr(error))
    )
  }

  test("drop with audit logs success info") {
    val gatewayMock = mock[ViewGateway]
    val gatewayWithAudit =
      new ImpalaViewGatewayWithAudit(gatewayMock, audit)

    inSequence(
      (audit.info _).expects(
        "Executing DropImpalaView(ImpalaView(database,viewName,List(),Some(ImpalaEntityImpl(db,table,List())),None))"),
      (audit.info _).expects(
        "DropImpalaView(ImpalaView(database,viewName,List(),Some(ImpalaEntityImpl(db,table,List())),None)) completed successfully")
    )

    val connectionConfig =
      UsernamePasswordConnectionConfig("host", "port", "schema", "user", "password", useSSL = true)
    val impalaView = ImpalaView(
      "database",
      "viewName",
      Seq.empty,
      readsFromSource = Some(ImpalaEntityImpl("db", "table", Seq.empty)),
      None)
    val entityResource = ImpalaEntityResource(impalaView, "jdbc://")

    (gatewayMock.drop _).expects(connectionConfig, impalaView, *).returns(Right(entityResource))

    assert(
      gatewayWithAudit.drop(connectionConfig, impalaView, ifExists = true) == Right(entityResource)
    )
  }

  test("drop with audit logs error info") {
    val gatewayMock = mock[ViewGateway]
    val gatewayWithAudit =
      new ImpalaViewGatewayWithAudit(gatewayMock, audit)

    inSequence(
      (audit.info _).expects(
        "Executing DropImpalaView(ImpalaView(database,viewName,List(),Some(ImpalaEntityImpl(db,table,List())),None))"),
      (audit.error _).expects(where { s: String =>
        s.startsWith(
          "DropImpalaView(ImpalaView(database,viewName,List(),Some(ImpalaEntityImpl(db,table,List())),None)) failed. Details: ExecuteDDLErr(")
      })
    )

    val connectionConfig =
      UsernamePasswordConnectionConfig("host", "port", "schema", "user", "password", useSSL = true)
    val impalaView = ImpalaView(
      "database",
      "viewName",
      Seq.empty,
      readsFromSource = Some(ImpalaEntityImpl("db", "table", Seq.empty)),
      None)

    val error = new SQLException("Error")
    (gatewayMock.drop _)
      .expects(connectionConfig, impalaView, *)
      .returns(Left(ExecuteDDLErr(error)))

    assert(
      gatewayWithAudit.drop(connectionConfig, impalaView, ifExists = true) ==
        Left(ExecuteDDLErr(error))
    )
  }
}
