package it.agilelab.provisioning.impala.table.provisioner.gateway.table

import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider.ConnectionConfig
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.ddl.ImpalaDataDefinitionLanguageProvider
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.query.DefaultSQLGateway
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaFormat.Parquet
import it.agilelab.provisioning.impala.table.provisioner.core.model.{
  ExternalTable,
  Field,
  ImpalaDataType
}
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

class ImpalaTableOutputPortGatewayTest extends AnyFunSuite with MockFactory {

  test("create") {
    val sqlQueryExecutor = mock[DefaultSQLGateway]
    (sqlQueryExecutor.executeDDLs _)
      .expects(
        ConnectionConfig("a", "b", "c", "deployUser", "deployPwd"),
        Seq(
          "CREATE DATABASE IF NOT EXISTS db",
          "CREATE EXTERNAL TABLE db.table (id INT) PARTITIONED BY (p1 INT) " +
            "STORED AS PARQUET LOCATION 'loc' TBLPROPERTIES ('impala.disableHmsSync'='false')"
        )
      )
      .once()
      .returns(Right(1))

    assert(
      new ImpalaExternalTableGateway(
        "deployUser",
        "deployPwd",
        new ImpalaDataDefinitionLanguageProvider(),
        sqlQueryExecutor
      ).create(
        ConnectionConfig("a", "b", "c", "", ""),
        ExternalTable(
          "db",
          "table",
          Seq(Field("id", ImpalaDataType.ImpalaInt, None)),
          Seq(Field("p1", ImpalaDataType.ImpalaInt, None)),
          "loc",
          Parquet
        ),
        ifNotExists = false
      ) == Right())
  }

  test("create if not exists") {
    val sqlQueryExecutor = mock[DefaultSQLGateway]
    (sqlQueryExecutor.executeDDLs _)
      .expects(
        ConnectionConfig("a", "b", "c", "deployUser", "deployPwd"),
        Seq(
          "CREATE DATABASE IF NOT EXISTS db",
          "CREATE EXTERNAL TABLE IF NOT EXISTS db.table (id INT) PARTITIONED BY (p1 INT) " +
            "STORED AS PARQUET LOCATION 'loc' TBLPROPERTIES ('impala.disableHmsSync'='false')"
        )
      )
      .once()
      .returns(Right(1))

    assert(
      new ImpalaExternalTableGateway(
        "deployUser",
        "deployPwd",
        new ImpalaDataDefinitionLanguageProvider(),
        sqlQueryExecutor
      ).create(
        ConnectionConfig("a", "b", "c", "", ""),
        ExternalTable(
          "db",
          "table",
          Seq(Field("id", ImpalaDataType.ImpalaInt, None)),
          Seq(Field("p1", ImpalaDataType.ImpalaInt, None)),
          "loc",
          Parquet
        ),
        ifNotExists = true
      ) == Right())
  }

  test("drop") {
    val sqlQueryExecutor = mock[DefaultSQLGateway]
    (sqlQueryExecutor.executeDDL _)
      .expects(
        ConnectionConfig("a", "b", "c", "deployUser", "deployPwd"),
        "DROP TABLE db.table"
      )
      .once()
      .returns(Right(1))

    assert(
      new ImpalaExternalTableGateway(
        "deployUser",
        "deployPwd",
        new ImpalaDataDefinitionLanguageProvider(),
        sqlQueryExecutor
      ).drop(
        ConnectionConfig("a", "b", "c", "", ""),
        ExternalTable(
          "db",
          "table",
          Seq(Field("id", ImpalaDataType.ImpalaInt, None)),
          Seq(Field("p1", ImpalaDataType.ImpalaInt, None)),
          "loc",
          Parquet
        ),
        ifExists = false
      ) == Right())
  }

  test("drop if exists") {
    val sqlQueryExecutor = mock[DefaultSQLGateway]
    (sqlQueryExecutor.executeDDL _)
      .expects(
        ConnectionConfig("a", "b", "c", "deployUser", "deployPwd"),
        "DROP TABLE IF EXISTS db.table"
      )
      .once()
      .returns(Right(1))

    assert(
      new ImpalaExternalTableGateway(
        "deployUser",
        "deployPwd",
        new ImpalaDataDefinitionLanguageProvider(),
        sqlQueryExecutor
      ).drop(
        ConnectionConfig("a", "b", "c", "", ""),
        ExternalTable(
          "db",
          "table",
          Seq(Field("id", ImpalaDataType.ImpalaInt, None)),
          Seq(Field("p1", ImpalaDataType.ImpalaInt, None)),
          "loc",
          Parquet
        ),
        ifExists = true
      ) == Right())
  }
}
