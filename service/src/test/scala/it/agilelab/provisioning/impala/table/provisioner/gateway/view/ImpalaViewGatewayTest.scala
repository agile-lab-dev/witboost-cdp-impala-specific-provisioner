package it.agilelab.provisioning.impala.table.provisioner.gateway.view

import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider.UsernamePasswordConnectionConfig
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.ddl.ImpalaDataDefinitionLanguageProvider
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.query.SqlGatewayError.ExecuteDDLErr
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.query.{
  DefaultSQLGateway,
  SqlGateway,
  SqlGatewayError
}
import it.agilelab.provisioning.impala.table.provisioner.core.model.{
  Field,
  ImpalaDataType,
  ImpalaView
}
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

import java.sql.SQLException

class ImpalaViewGatewayTest extends AnyFunSuite with MockFactory {
  test("create") {
    val sqlQueryExecutor = mock[DefaultSQLGateway]
    (sqlQueryExecutor.executeDDL _)
      .expects(
        UsernamePasswordConnectionConfig("a", "b", "c", "deployUser", "deployPwd", useSSL = true),
        "CREATE VIEW db.view AS SELECT id FROM db.table"
      )
      .once()
      .returns(Right(1))

    assert(
      new ImpalaViewGateway(
        "deployUser",
        "deployPwd",
        new ImpalaDataDefinitionLanguageProvider(),
        sqlQueryExecutor
      ).create(
        UsernamePasswordConnectionConfig("a", "b", "c", "", "", useSSL = true),
        ImpalaView(
          database = "db",
          name = "view",
          schema = Seq(Field("id", ImpalaDataType.ImpalaInt, None)),
          readsFromSourceName = Some("table"),
          querySourceStatement = None
        ),
        ifNotExists = false
      ) == Right())
  }

  test("create if not exists") {
    val sqlQueryExecutor = mock[DefaultSQLGateway]
    (sqlQueryExecutor.executeDDL _)
      .expects(
        UsernamePasswordConnectionConfig("a", "b", "c", "deployUser", "deployPwd", useSSL = true),
        "CREATE VIEW IF NOT EXISTS db.view AS SELECT id FROM db.table"
      )
      .once()
      .returns(Right(1))

    assert(
      new ImpalaViewGateway(
        "deployUser",
        "deployPwd",
        new ImpalaDataDefinitionLanguageProvider(),
        sqlQueryExecutor
      ).create(
        UsernamePasswordConnectionConfig("a", "b", "c", "", "", useSSL = true),
        ImpalaView(
          database = "db",
          name = "view",
          schema = Seq(Field("id", ImpalaDataType.ImpalaInt, None)),
          readsFromSourceName = Some("table"),
          querySourceStatement = None
        ),
        ifNotExists = true
      ) == Right())
  }

  test("create returns Left if Sql gateway failed") {

    val error = new SQLException("error")

    val sqlQueryExecutor = mock[SqlGateway]
    (sqlQueryExecutor.executeDDL _)
      .expects(
        UsernamePasswordConnectionConfig("a", "b", "c", "deployUser", "deployPwd", useSSL = true),
        "CREATE VIEW IF NOT EXISTS db.view AS SELECT id FROM db.table"
      )
      .once()
      .returns(Left(ExecuteDDLErr(error)))

    assert(
      new ImpalaViewGateway(
        "deployUser",
        "deployPwd",
        new ImpalaDataDefinitionLanguageProvider(),
        sqlQueryExecutor
      ).create(
        UsernamePasswordConnectionConfig("a", "b", "c", "", "", useSSL = true),
        ImpalaView(
          database = "db",
          name = "view",
          schema = Seq(Field("id", ImpalaDataType.ImpalaInt, None)),
          readsFromSourceName = Some("table"),
          querySourceStatement = None
        ),
        ifNotExists = true
      ) == Left(ExecuteDDLErr(error)))
  }

  test("drop") {
    val sqlQueryExecutor = mock[DefaultSQLGateway]
    (sqlQueryExecutor.executeDDL _)
      .expects(
        UsernamePasswordConnectionConfig("a", "b", "c", "deployUser", "deployPwd", useSSL = true),
        "DROP VIEW db.view"
      )
      .once()
      .returns(Right(1))

    assert(
      new ImpalaViewGateway(
        "deployUser",
        "deployPwd",
        new ImpalaDataDefinitionLanguageProvider(),
        sqlQueryExecutor
      ).drop(
        UsernamePasswordConnectionConfig("a", "b", "c", "", "", useSSL = true),
        ImpalaView(
          database = "db",
          name = "view",
          schema = Seq(Field("id", ImpalaDataType.ImpalaInt, None)),
          readsFromSourceName = Some("table"),
          querySourceStatement = None
        ),
        ifExists = false
      ) == Right())
  }

  test("drop if exists") {
    val sqlQueryExecutor = mock[DefaultSQLGateway]
    (sqlQueryExecutor.executeDDL _)
      .expects(
        UsernamePasswordConnectionConfig("a", "b", "c", "deployUser", "deployPwd", useSSL = true),
        "DROP VIEW IF EXISTS db.view"
      )
      .once()
      .returns(Right(1))

    assert(
      new ImpalaViewGateway(
        "deployUser",
        "deployPwd",
        new ImpalaDataDefinitionLanguageProvider(),
        sqlQueryExecutor
      ).drop(
        UsernamePasswordConnectionConfig("a", "b", "c", "", "", useSSL = true),
        ImpalaView(
          database = "db",
          name = "view",
          schema = Seq(Field("id", ImpalaDataType.ImpalaInt, None)),
          readsFromSourceName = Some("table"),
          querySourceStatement = None
        ),
        ifExists = true
      ) == Right())
  }

  test("drop returns Left if Sql gateway failed") {

    val error = new SQLException("error")

    val sqlQueryExecutor = mock[SqlGateway]
    (sqlQueryExecutor.executeDDL _)
      .expects(
        UsernamePasswordConnectionConfig("a", "b", "c", "deployUser", "deployPwd", useSSL = true),
        "DROP VIEW IF EXISTS db.view"
      )
      .once()
      .returns(Left(ExecuteDDLErr(error)))

    assert(
      new ImpalaViewGateway(
        "deployUser",
        "deployPwd",
        new ImpalaDataDefinitionLanguageProvider(),
        sqlQueryExecutor
      ).drop(
        UsernamePasswordConnectionConfig("a", "b", "c", "", "", useSSL = true),
        ImpalaView(
          database = "db",
          name = "view",
          schema = Seq(Field("id", ImpalaDataType.ImpalaInt, None)),
          readsFromSourceName = Some("table"),
          querySourceStatement = None
        ),
        ifExists = true
      ) == Left(ExecuteDDLErr(error)))
  }
}
