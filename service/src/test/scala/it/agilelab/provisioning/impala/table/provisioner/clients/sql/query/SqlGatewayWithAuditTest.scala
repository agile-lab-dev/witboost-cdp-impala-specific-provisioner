package it.agilelab.provisioning.impala.table.provisioner.clients.sql.query

import it.agilelab.provisioning.commons.audit.Audit
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider.ConnectionProviderError.ParseConnectionStringErr
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider.UsernamePasswordConnectionConfig
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.query.SqlGatewayError.{
  ConnectionErr,
  ExecuteDDLErr
}
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

import java.sql.SQLException

class SqlGatewayWithAuditTest extends AnyFunSuite with MockFactory {

  val defaultSqlClient = stub[SqlGateway]
  val audit: Audit = mock[Audit]
  val sqlClient = new SqlGatewayWithAudit(defaultSqlClient, audit)

  test("executeDDL logs success info") {
    val sql = "select id, col1, col2, col3 from very_very_very_very_very_long_table"
    val connectionConfig = UsernamePasswordConnectionConfig("a", "b", "c", "d", "s", useSSL = true)

    inSequence(
      (audit.info _).expects(
        "Executing statement ExecuteDDL(select id, col1, col2, col3 from very_very_very_ve...)"),
      (audit.info _).expects(
        "ExecuteDDL(select id, col1, col2, col3 from very_very_very_ve...) completed successfully")
    )

    (defaultSqlClient.executeDDL _)
      .when(connectionConfig, sql)
      .returns(Right(1))

    val actual = sqlClient.executeDDL(connectionConfig, sql)
    assert(actual == Right(1))
  }

  test("executeDDL logs error info") {
    val sql = "select id, col1, col2, col3 from very_very_very_very_very_long_table"
    val connectionConfig = UsernamePasswordConnectionConfig("a", "b", "c", "d", "s", useSSL = true)
    val error = ExecuteDDLErr(new SQLException("Error"))
    inSequence(
      (audit.info _).expects(
        "Executing statement ExecuteDDL(select id, col1, col2, col3 from very_very_very_ve...)"),
      (audit.error _).expects(where { s: String =>
        s.startsWith(
          "ExecuteDDL(select id, col1, col2, col3 from very_very_very_ve...) failed. Details: ExecuteDDLErr(")
      })
    )
    (defaultSqlClient.executeDDL _)
      .when(connectionConfig, sql)
      .returns(Left(error))

    val actual = sqlClient.executeDDL(connectionConfig, sql)
    assert(actual == Left(error))
  }

  test("getConnectionString logs success info") {
    val connectionConfig = UsernamePasswordConnectionConfig("a", "b", "c", "d", "s", useSSL = true)

    inSequence(
      (audit.info _).expects(
        "Executing statement GetConnectionString(UsernamePasswordConnectionConfig(a, b, c, d, s, useSSL=true))"),
      (audit.info _).expects(
        "GetConnectionString(UsernamePasswordConnectionConfig(a, b, c, d, s, useSSL=true)) completed successfully")
    )

    (defaultSqlClient.getConnectionString _)
      .when(connectionConfig)
      .returns(Right("jdbc://"))

    val actual = sqlClient.getConnectionString(connectionConfig)
    assert(actual == Right("jdbc://"))
  }

  test("getConnectionString logs error info") {
    val connectionConfig = UsernamePasswordConnectionConfig("a", "b", "c", "d", "s", useSSL = true)
    val error = ParseConnectionStringErr(connectionConfig, "jdbc://")
    inSequence(
      (audit.info _).expects(
        "Executing statement GetConnectionString(UsernamePasswordConnectionConfig(a, b, c, d, s, useSSL=true))"),
      (audit.error _).expects(
        "GetConnectionString(UsernamePasswordConnectionConfig(a, b, c, d, s, useSSL=true)) failed. Details: ConnectionErr(ParseConnectionStringErr(UsernamePasswordConnectionConfig(a, b, c, d, s, useSSL=true), jdbc://))"
      )
    )

    (defaultSqlClient.getConnectionString _)
      .when(connectionConfig)
      .returns(Left(ConnectionErr(error)))

    val actual = sqlClient.getConnectionString(connectionConfig)
    assert(actual == Left(ConnectionErr(error)))
  }
}
