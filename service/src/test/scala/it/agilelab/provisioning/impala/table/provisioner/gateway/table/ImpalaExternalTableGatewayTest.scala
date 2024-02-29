package it.agilelab.provisioning.impala.table.provisioner.gateway.table

import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider.UsernamePasswordConnectionConfig
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.ddl.ImpalaDataDefinitionLanguageProvider
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.query.SqlGateway
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.query.SqlGatewayError.ExecuteDDLErr
import it.agilelab.provisioning.impala.table.provisioner.core.model.{ ExternalTable, ImpalaFormat }
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

import java.sql.SQLException

class ImpalaExternalTableGatewayTest extends AnyFunSuite with MockFactory {

  test("create") {
    val sqlGateway: SqlGateway = mock[SqlGateway]
    val mockDDLProvider: ImpalaDataDefinitionLanguageProvider =
      mock[ImpalaDataDefinitionLanguageProvider]

    val tableGateway =
      new ImpalaExternalTableGateway("deployUser", "deployPassword", mockDDLProvider, sqlGateway)

    val connectionConfig =
      UsernamePasswordConnectionConfig(
        "host",
        "port",
        "schema",
        "deployUser",
        "deployPassword",
        useSSL = true)
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

    inSequence(
      (mockDDLProvider.createDataBase _)
        .expects(*, *)
        .returns("CREATE DATABASE ..."),
      (mockDDLProvider.createExternalTable _)
        .expects(*, *)
        .returns("CREATE EXTERNAL TABLE ...")
    )
    (sqlGateway.executeDDLs _).expects(connectionConfig, *).returns(Right(1))

    assert(
      tableGateway.create(connectionConfig, externalTable, ifNotExists = true) == Right(())
    )
  }

  test("create returns Left() if sql execution failed") {
    val sqlGateway: SqlGateway = mock[SqlGateway]
    val mockDDLProvider: ImpalaDataDefinitionLanguageProvider =
      mock[ImpalaDataDefinitionLanguageProvider]

    val tableGateway =
      new ImpalaExternalTableGateway("deployUser", "deployPassword", mockDDLProvider, sqlGateway)

    val connectionConfig =
      UsernamePasswordConnectionConfig(
        "host",
        "port",
        "schema",
        "deployUser",
        "deployPassword",
        useSSL = true)
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

    val error = ExecuteDDLErr(new SQLException("Error!"))
    inSequence(
      (mockDDLProvider.createDataBase _)
        .expects(*, *)
        .returns("CREATE DATABASE ..."),
      (mockDDLProvider.createExternalTable _)
        .expects(*, *)
        .returns("CREATE EXTERNAL TABLE ...")
    )
    (sqlGateway.executeDDLs _)
      .expects(connectionConfig, *)
      .returns(Left(error))

    assert(tableGateway.create(connectionConfig, externalTable, ifNotExists = true) == Left(error))
  }

  test("drop") {
    val sqlGateway: SqlGateway = mock[SqlGateway]
    val mockDDLProvider: ImpalaDataDefinitionLanguageProvider =
      mock[ImpalaDataDefinitionLanguageProvider]

    val tableGateway =
      new ImpalaExternalTableGateway("deployUser", "deployPassword", mockDDLProvider, sqlGateway)

    val connectionConfig =
      UsernamePasswordConnectionConfig(
        "host",
        "port",
        "schema",
        "deployUser",
        "deployPassword",
        useSSL = true)
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

    (mockDDLProvider.dropExternalTable _)
      .expects(*, *)
      .returns("DROP TABLE ...")

    (sqlGateway.executeDDL _).expects(connectionConfig, *).returns(Right(1))

    assert(
      tableGateway.drop(connectionConfig, externalTable, ifExists = true) == Right(())
    )
  }

  test("drop returns Left() if sql execution failed") {
    val sqlGateway: SqlGateway = mock[SqlGateway]
    val mockDDLProvider: ImpalaDataDefinitionLanguageProvider =
      mock[ImpalaDataDefinitionLanguageProvider]

    val tableGateway =
      new ImpalaExternalTableGateway("deployUser", "deployPassword", mockDDLProvider, sqlGateway)

    val connectionConfig =
      UsernamePasswordConnectionConfig(
        "host",
        "port",
        "schema",
        "deployUser",
        "deployPassword",
        useSSL = true)
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
    val error = ExecuteDDLErr(new SQLException("Error!"))

    (mockDDLProvider.dropExternalTable _)
      .expects(*, *)
      .returns("DROP TABLE ...")

    (sqlGateway.executeDDL _).expects(connectionConfig, *).returns(Left(error))

    assert(tableGateway.drop(connectionConfig, externalTable, ifExists = true) == Left(error))

  }
}
