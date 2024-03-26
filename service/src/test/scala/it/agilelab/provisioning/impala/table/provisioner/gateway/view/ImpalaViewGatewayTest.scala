package it.agilelab.provisioning.impala.table.provisioner.gateway.view

import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider.UsernamePasswordConnectionConfig
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.ddl.ImpalaDataDefinitionLanguageProvider
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.query.SqlGatewayError.ExecuteDDLErr
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.query.{
  DefaultSQLGateway,
  SqlGateway
}
import it.agilelab.provisioning.impala.table.provisioner.core.model.{
  Field,
  ImpalaDataType,
  ImpalaEntityImpl,
  ImpalaEntityResource,
  ImpalaView
}
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

import java.sql.SQLException

class ImpalaViewGatewayTest extends AnyFunSuite with MockFactory {
  test("create") {
    val connectionConfig = UsernamePasswordConnectionConfig("a", "b", "c", "", "", useSSL = true)

    val sqlQueryExecutor = mock[DefaultSQLGateway]
    (sqlQueryExecutor.executeDDLs _)
      .expects(
        UsernamePasswordConnectionConfig("a", "b", "c", "deployUser", "deployPwd", useSSL = true),
        Seq("CREATE DATABASE db", "CREATE VIEW db.view AS SELECT id FROM db.table")
      )
      .once()
      .returns(Right(1))
    (sqlQueryExecutor.getConnectionString _).expects(connectionConfig).returns(Right("jdbc://"))

    val view = ImpalaView(
      database = "db",
      name = "view",
      schema = Seq(Field("id", ImpalaDataType.ImpalaInt, None)),
      readsFromSource = Some(ImpalaEntityImpl("db", "table", Seq.empty)),
      querySourceStatement = None
    )

    assert(
      new ImpalaViewGateway(
        "deployUser",
        "deployPwd",
        new ImpalaDataDefinitionLanguageProvider(),
        sqlQueryExecutor
      ).create(
        connectionConfig,
        view,
        ifNotExists = false
      ) == Right(ImpalaEntityResource(view, "jdbc://")))
  }

  test("create if not exists") {
    val connectionConfig = UsernamePasswordConnectionConfig("a", "b", "c", "", "", useSSL = true)

    val sqlQueryExecutor = mock[DefaultSQLGateway]
    (sqlQueryExecutor.executeDDLs _)
      .expects(
        UsernamePasswordConnectionConfig("a", "b", "c", "deployUser", "deployPwd", useSSL = true),
        Seq(
          "CREATE DATABASE IF NOT EXISTS db",
          "CREATE VIEW IF NOT EXISTS db.view AS SELECT id FROM db.table")
      )
      .once()
      .returns(Right(1))

    (sqlQueryExecutor.getConnectionString _).expects(connectionConfig).returns(Right("jdbc://"))

    val view = ImpalaView(
      database = "db",
      name = "view",
      schema = Seq(Field("id", ImpalaDataType.ImpalaInt, None)),
      readsFromSource = Some(ImpalaEntityImpl("db", "table", Seq.empty)),
      querySourceStatement = None
    )

    assert(
      new ImpalaViewGateway(
        "deployUser",
        "deployPwd",
        new ImpalaDataDefinitionLanguageProvider(),
        sqlQueryExecutor
      ).create(
        connectionConfig,
        view,
        ifNotExists = true
      ) == Right(ImpalaEntityResource(view, "jdbc://")))
  }

  test("create returns Left if Sql gateway failed") {

    val error = new SQLException("error")

    val sqlQueryExecutor = mock[SqlGateway]
    (sqlQueryExecutor.executeDDLs _)
      .expects(
        UsernamePasswordConnectionConfig("a", "b", "c", "deployUser", "deployPwd", useSSL = true),
        Seq(
          "CREATE DATABASE IF NOT EXISTS db",
          "CREATE VIEW IF NOT EXISTS db.view AS SELECT id FROM db.table")
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
          readsFromSource = Some(ImpalaEntityImpl("db", "table", Seq.empty)),
          querySourceStatement = None
        ),
        ifNotExists = true
      ) == Left(ExecuteDDLErr(error)))
  }

  test("drop") {
    val connectionConfig = UsernamePasswordConnectionConfig("a", "b", "c", "", "", useSSL = true)
    val sqlQueryExecutor = mock[DefaultSQLGateway]
    (sqlQueryExecutor.executeDDL _)
      .expects(
        UsernamePasswordConnectionConfig("a", "b", "c", "deployUser", "deployPwd", useSSL = true),
        "DROP VIEW db.view"
      )
      .once()
      .returns(Right(1))

    (sqlQueryExecutor.getConnectionString _).expects(connectionConfig).returns(Right("jdbc://"))

    val view = ImpalaView(
      database = "db",
      name = "view",
      schema = Seq(Field("id", ImpalaDataType.ImpalaInt, None)),
      readsFromSource = Some(ImpalaEntityImpl("db", "table", Seq.empty)),
      querySourceStatement = None
    )

    assert(
      new ImpalaViewGateway(
        "deployUser",
        "deployPwd",
        new ImpalaDataDefinitionLanguageProvider(),
        sqlQueryExecutor
      ).drop(
        connectionConfig,
        view,
        ifExists = false
      ) == Right(ImpalaEntityResource(view, "jdbc://")))
  }

  test("drop if exists") {
    val connectionConfig = UsernamePasswordConnectionConfig("a", "b", "c", "", "", useSSL = true)
    val sqlQueryExecutor = mock[DefaultSQLGateway]
    (sqlQueryExecutor.executeDDL _)
      .expects(
        UsernamePasswordConnectionConfig("a", "b", "c", "deployUser", "deployPwd", useSSL = true),
        "DROP VIEW IF EXISTS db.view"
      )
      .once()
      .returns(Right(1))

    (sqlQueryExecutor.getConnectionString _).expects(connectionConfig).returns(Right("jdbc://"))

    val view = ImpalaView(
      database = "db",
      name = "view",
      schema = Seq(Field("id", ImpalaDataType.ImpalaInt, None)),
      readsFromSource = Some(ImpalaEntityImpl("db", "table", Seq.empty)),
      querySourceStatement = None
    )

    assert(
      new ImpalaViewGateway(
        "deployUser",
        "deployPwd",
        new ImpalaDataDefinitionLanguageProvider(),
        sqlQueryExecutor
      ).drop(
        connectionConfig,
        view,
        ifExists = true
      ) == Right(ImpalaEntityResource(view, "jdbc://")))
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
          readsFromSource = Some(ImpalaEntityImpl("db", "table", Seq.empty)),
          querySourceStatement = None
        ),
        ifExists = true
      ) == Left(ExecuteDDLErr(error)))
  }
}
