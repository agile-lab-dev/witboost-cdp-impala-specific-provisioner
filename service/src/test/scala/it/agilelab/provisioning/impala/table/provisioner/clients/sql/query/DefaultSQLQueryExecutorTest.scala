package it.agilelab.provisioning.impala.table.provisioner.clients.sql.query

import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider.{
  ConnectionConfig,
  ConnectionProvider
}
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider.{
  ConnectionConfig,
  ConnectionProvider
}
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

import java.sql.{ Connection, Statement }

class DefaultSQLQueryExecutorTest extends AnyFunSuite with MockFactory {

  test("executeDDL") {
    val connection = mock[Connection]
    val statement = mock[Statement]
    val connectionProvider = mock[ConnectionProvider]

    (connectionProvider
      .get(_: ConnectionConfig))
      .expects(ConnectionConfig("a", "b", "c", "d", "s"))
      .returns(connection)

    (connection.createStatement _: () => Statement).expects().returns(statement)
    (statement.executeUpdate(_: String)).expects("xyz").once()
    (connection.close _).expects().once()

    new DefaultSQLGateway(
      connectionProvider
    ).executeDDL(ConnectionConfig("a", "b", "c", "d", "s"), "xyz")

  }

  test("executeDDLs") {
    val connection = mock[Connection]
    val statement = mock[Statement]
    val connectionProvider = mock[ConnectionProvider]

    (connectionProvider
      .get(_: ConnectionConfig))
      .expects(ConnectionConfig("a", "b", "c", "d", "s"))
      .returns(connection)

    (connection.createStatement _: () => Statement).expects().returns(statement)
    (statement.executeUpdate(_: String)).expects("abc").once()
    (statement.executeUpdate(_: String)).expects("xyz").once()
    (connection.close _).expects().once()

    new DefaultSQLGateway(
      connectionProvider
    ).executeDDLs(ConnectionConfig("a", "b", "c", "d", "s"), Seq("abc", "xyz"))

  }

}
