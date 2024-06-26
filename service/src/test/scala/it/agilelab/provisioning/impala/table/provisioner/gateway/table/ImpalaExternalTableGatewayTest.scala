package it.agilelab.provisioning.impala.table.provisioner.gateway.table

import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider.UsernamePasswordConnectionConfig
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.ddl.ImpalaDataDefinitionLanguageProvider
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.query.SqlGateway
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.query.SqlGatewayError.ExecuteDDLErr
import it.agilelab.provisioning.impala.table.provisioner.core.model.{
  ExternalTable,
  ImpalaEntityResource,
  ImpalaFormat
}
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
    (sqlGateway.getConnectionString _)
      .expects(connectionConfig.setCredentials("<USER>", "<PASSWORD>"))
      .returns(Right("jdbc://"))

    assert(
      tableGateway.create(connectionConfig, externalTable, ifNotExists = true) == Right(
        ImpalaEntityResource(externalTable, "jdbc://"))
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

  test("refresh") {

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

    (mockDDLProvider.refreshStatements _)
      .expects(*)
      .returns(Seq("INVALIDATE METADATA ...", "ALTER TABLE ... RECOVER PARTITIONS"))

    (sqlGateway.executeDDLs _).expects(connectionConfig, *).returns(Right(1))
    (sqlGateway.getConnectionString _)
      .expects(connectionConfig.setCredentials("<USER>", "<PASSWORD>"))
      .returns(Right("jdbc://"))

    assert(
      tableGateway.refresh(connectionConfig, externalTable) == Right(
        ImpalaEntityResource(externalTable, "jdbc://"))
    )
  }

  test("refresh returns Left() if sql execution failed") {
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
    (mockDDLProvider.refreshStatements _)
      .expects(*)
      .returns(Seq("INVALIDATE METADATA ...", "ALTER TABLE ... RECOVER PARTITIONS"))
    (sqlGateway.executeDDLs _)
      .expects(connectionConfig, *)
      .returns(Left(error))

    assert(tableGateway.refresh(connectionConfig, externalTable) == Left(error))
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
    (sqlGateway.getConnectionString _).expects(connectionConfig).returns(Right("jdbc://"))

    assert(
      tableGateway.drop(connectionConfig, externalTable, ifExists = true) == Right(
        ImpalaEntityResource(externalTable, "jdbc://"))
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
