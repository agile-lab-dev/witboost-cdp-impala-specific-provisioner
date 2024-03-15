package it.agilelab.provisioning.impala.table.provisioner.gateway.table

import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider.UsernamePasswordConnectionConfig
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.ddl.ImpalaDataDefinitionLanguageProvider
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.query.DefaultSQLGateway
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaFormat.Parquet
import it.agilelab.provisioning.impala.table.provisioner.core.model.{
  ExternalTable,
  Field,
  ImpalaDataType,
  ImpalaEntityResource
}
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

class ImpalaTableOutputPortGatewayTest extends AnyFunSuite with MockFactory {

  test("create") {
    val connectionConfig = UsernamePasswordConnectionConfig("a", "b", "c", "", "", useSSL = true)

    val sqlQueryExecutor = mock[DefaultSQLGateway]
    (sqlQueryExecutor.executeDDLs _)
      .expects(
        UsernamePasswordConnectionConfig("a", "b", "c", "deployUser", "deployPwd", useSSL = true),
        Seq(
          "CREATE DATABASE IF NOT EXISTS db",
          "CREATE EXTERNAL TABLE db.table (id INT) PARTITIONED BY (p1 INT) " +
            "STORED AS PARQUET LOCATION 'loc' TBLPROPERTIES ('impala.disableHmsSync'='false')"
        )
      )
      .once()
      .returns(Right(1))
    (sqlQueryExecutor.getConnectionString _)
      .expects(connectionConfig.setCredentials("<USER>", "<PASSWORD>"))
      .returns(Right("jdbc://"))

    val externalTable = ExternalTable(
      "db",
      "table",
      Seq(Field("id", ImpalaDataType.ImpalaInt, None)),
      Seq(Field("p1", ImpalaDataType.ImpalaInt, None)),
      "loc",
      Parquet,
      None,
      Map.empty,
      header = false
    )

    assert(
      new ImpalaExternalTableGateway(
        "deployUser",
        "deployPwd",
        new ImpalaDataDefinitionLanguageProvider(),
        sqlQueryExecutor
      ).create(
        connectionConfig,
        externalTable,
        ifNotExists = false
      ) == Right(ImpalaEntityResource(externalTable, "jdbc://")))
  }

  test("create if not exists") {
    val connectionConfig = UsernamePasswordConnectionConfig("a", "b", "c", "", "", useSSL = true)

    val sqlQueryExecutor = mock[DefaultSQLGateway]
    (sqlQueryExecutor.executeDDLs _)
      .expects(
        UsernamePasswordConnectionConfig("a", "b", "c", "deployUser", "deployPwd", useSSL = true),
        Seq(
          "CREATE DATABASE IF NOT EXISTS db",
          "CREATE EXTERNAL TABLE IF NOT EXISTS db.table (id INT) PARTITIONED BY (p1 INT) " +
            "STORED AS PARQUET LOCATION 'loc' TBLPROPERTIES ('impala.disableHmsSync'='false')"
        )
      )
      .once()
      .returns(Right(1))
    (sqlQueryExecutor.getConnectionString _)
      .expects(connectionConfig.setCredentials("<USER>", "<PASSWORD>"))
      .returns(Right("jdbc://"))

    val externalTable = ExternalTable(
      "db",
      "table",
      Seq(Field("id", ImpalaDataType.ImpalaInt, None)),
      Seq(Field("p1", ImpalaDataType.ImpalaInt, None)),
      "loc",
      Parquet,
      None,
      Map.empty,
      header = false
    )

    assert(
      new ImpalaExternalTableGateway(
        "deployUser",
        "deployPwd",
        new ImpalaDataDefinitionLanguageProvider(),
        sqlQueryExecutor
      ).create(
        connectionConfig,
        externalTable,
        ifNotExists = true
      ) == Right(ImpalaEntityResource(externalTable, "jdbc://")))
  }

  test("drop") {
    val connectionConfig = UsernamePasswordConnectionConfig("a", "b", "c", "", "", useSSL = true)

    val sqlQueryExecutor = mock[DefaultSQLGateway]
    (sqlQueryExecutor.executeDDL _)
      .expects(
        UsernamePasswordConnectionConfig("a", "b", "c", "deployUser", "deployPwd", useSSL = true),
        "DROP TABLE db.table"
      )
      .once()
      .returns(Right(1))
    (sqlQueryExecutor.getConnectionString _).expects(connectionConfig).returns(Right("jdbc://"))

    val externalTable = ExternalTable(
      "db",
      "table",
      Seq(Field("id", ImpalaDataType.ImpalaInt, None)),
      Seq(Field("p1", ImpalaDataType.ImpalaInt, None)),
      "loc",
      Parquet,
      None,
      Map.empty,
      header = false
    )

    assert(
      new ImpalaExternalTableGateway(
        "deployUser",
        "deployPwd",
        new ImpalaDataDefinitionLanguageProvider(),
        sqlQueryExecutor
      ).drop(
        connectionConfig,
        externalTable,
        ifExists = false
      ) == Right(ImpalaEntityResource(externalTable, "jdbc://")))
  }

  test("drop if exists") {
    val connectionConfig = UsernamePasswordConnectionConfig("a", "b", "c", "", "", useSSL = true)

    val sqlQueryExecutor = mock[DefaultSQLGateway]
    (sqlQueryExecutor.executeDDL _)
      .expects(
        UsernamePasswordConnectionConfig("a", "b", "c", "deployUser", "deployPwd", useSSL = true),
        "DROP TABLE IF EXISTS db.table"
      )
      .once()
      .returns(Right(1))
    (sqlQueryExecutor.getConnectionString _).expects(connectionConfig).returns(Right("jdbc://"))

    val externalTable = ExternalTable(
      "db",
      "table",
      Seq(Field("id", ImpalaDataType.ImpalaInt, None)),
      Seq(Field("p1", ImpalaDataType.ImpalaInt, None)),
      "loc",
      Parquet,
      None,
      Map.empty,
      header = false
    )

    assert(
      new ImpalaExternalTableGateway(
        "deployUser",
        "deployPwd",
        new ImpalaDataDefinitionLanguageProvider(),
        sqlQueryExecutor
      ).drop(
        connectionConfig,
        externalTable,
        ifExists = true
      ) == Right(ImpalaEntityResource(externalTable, "jdbc://")))
  }
}
