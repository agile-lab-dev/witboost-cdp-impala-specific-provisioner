package it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection

import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider.ConnectionProviderError.ParseConnectionStringErr
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider.{
  ConnectionStringProvider,
  SQLConnectionProvider,
  UsernamePasswordConnectionConfig
}
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

class SQLConnectionProviderTest extends AnyFunSuite with MockFactory {

  test("get connection string returns Right") {
    val stringProvider = mock[ConnectionStringProvider]
    val connectionConfig = UsernamePasswordConnectionConfig("a", "b", "c", "", "", useSSL = true)

    val provider = new SQLConnectionProvider("driver", stringProvider)

    (stringProvider.get _).expects(connectionConfig).returns(Right("jdbc://"))

    val actual = provider.getConnectionString(connectionConfig)

    assert(actual == Right("jdbc://"))
  }

  test("get connection string returns Left if string provider failed") {
    val stringProvider = mock[ConnectionStringProvider]
    val connectionConfig = UsernamePasswordConnectionConfig("a", "b", "c", "", "", useSSL = true)
    val err = ParseConnectionStringErr(connectionConfig, "jdbc://")
    val provider = new SQLConnectionProvider("driver", stringProvider)

    (stringProvider.get _).expects(connectionConfig).returns(Left(err))

    val actual = provider.getConnectionString(connectionConfig)

    assert(actual == Left(err))
  }

}
