package it.agilelab.provisioning.impala.table.provisioner.clients.sql.query

import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider.ConnectionProviderError.ParseConnectionStringErr
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider.{
  ConnectionConfig,
  ConnectionProvider,
  UsernamePasswordConnectionConfig
}
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

import java.sql.{ Connection, SQLException, Statement }

class DefaultSQLGatewayTest extends AnyFunSuite with MockFactory {

  test("executeDDL") {
    val connection = mock[Connection]
    val statement = mock[Statement]
    val connectionProvider = mock[ConnectionProvider]

    (connectionProvider
      .get(_: ConnectionConfig))
      .expects(UsernamePasswordConnectionConfig("a", "b", "c", "d", "s", useSSL = true))
      .returns(Right(connection))

    (connection.createStatement _: () => Statement).expects().returns(statement)
    (statement.executeUpdate(_: String)).expects("xyz").once()
    (connection.close _).expects().once()

    assert(
      new DefaultSQLGateway(connectionProvider)
        .executeDDL(UsernamePasswordConnectionConfig("a", "b", "c", "d", "s", useSSL = true), "xyz")
        .isRight)
  }

  test("execute DDL fail getting the connection and returns Left()") {
    val connection = mock[Connection]
    val statement = mock[Statement]
    val connectionProvider = mock[ConnectionProvider]

    val connectionConfig = UsernamePasswordConnectionConfig("a", "b", "c", "d", "s", useSSL = true)

    (connectionProvider
      .get(_: ConnectionConfig))
      .expects(connectionConfig)
      .returns(Left(ParseConnectionStringErr(connectionConfig, "pattern")))

    assert(
      new DefaultSQLGateway(connectionProvider)
        .executeDDL(UsernamePasswordConnectionConfig("a", "b", "c", "d", "s", useSSL = true), "xyz")
        .isLeft)
  }

  test("execute DDL fail and returns Left()") {
    val connection = mock[Connection]
    val statement = mock[Statement]
    val connectionProvider = mock[ConnectionProvider]

    val connectionConfig = UsernamePasswordConnectionConfig("a", "b", "c", "d", "s", useSSL = true)

    (connectionProvider
      .get(_: ConnectionConfig))
      .expects(UsernamePasswordConnectionConfig("a", "b", "c", "d", "s", useSSL = true))
      .returns(Right(connection))

    (connection.createStatement _: () => Statement).expects().returns(statement).once()
    (statement.executeUpdate(_: String)).expects("xyz").once().throws(new SQLException("err"))
    (connection.close _).expects().once()

    assert(
      new DefaultSQLGateway(connectionProvider)
        .executeDDL(UsernamePasswordConnectionConfig("a", "b", "c", "d", "s", useSSL = true), "xyz")
        .isLeft)
  }

  test("executeDDLs") {
    val connection = mock[Connection]
    val statement = mock[Statement]
    val connectionProvider = mock[ConnectionProvider]

    (connectionProvider
      .get(_: ConnectionConfig))
      .expects(UsernamePasswordConnectionConfig("a", "b", "c", "d", "s", useSSL = true))
      .returns(Right(connection))

    (connection.createStatement _: () => Statement).expects().returns(statement).once()
    (statement.executeUpdate(_: String)).expects("abc").once()
    (statement.executeUpdate(_: String)).expects("xyz").once()
    (connection.close _).expects().once()

    val actual = new DefaultSQLGateway(connectionProvider)
      .executeDDLs(
        UsernamePasswordConnectionConfig("a", "b", "c", "d", "s", useSSL = true),
        Seq("abc", "xyz"))
    assert(actual.isRight)
  }

  test("execute DDLs fail getting the connection and returns Left()") {
    val connection = mock[Connection]
    val statement = mock[Statement]
    val connectionProvider = mock[ConnectionProvider]

    val connectionConfig = UsernamePasswordConnectionConfig("a", "b", "c", "d", "s", useSSL = true)

    (connectionProvider
      .get(_: ConnectionConfig))
      .expects(connectionConfig)
      .returns(Left(ParseConnectionStringErr(connectionConfig, "pattern")))

    assert(
      new DefaultSQLGateway(
        connectionProvider
      ).executeDDLs(
        UsernamePasswordConnectionConfig("a", "b", "c", "d", "s", useSSL = true),
        Seq("abc", "xyz"))
        .isLeft)
  }

  test("execute DDLs fail and returns Left()") {
    val connection = mock[Connection]
    val statement = mock[Statement]
    val connectionProvider = mock[ConnectionProvider]

    (connectionProvider
      .get(_: ConnectionConfig))
      .expects(UsernamePasswordConnectionConfig("a", "b", "c", "d", "s", useSSL = true))
      .returns(Right(connection))

    (connection.createStatement _: () => Statement).expects().returns(statement)
    (statement.executeUpdate(_: String)).expects("abc").once()
    (statement.executeUpdate(_: String)).expects("xyz").once().throws(new SQLException("error"))
    (connection.close _).expects().once()

    assert(
      new DefaultSQLGateway(
        connectionProvider
      ).executeDDLs(
        UsernamePasswordConnectionConfig("a", "b", "c", "d", "s", useSSL = true),
        Seq("abc", "xyz"))
        .isLeft)
  }

}
