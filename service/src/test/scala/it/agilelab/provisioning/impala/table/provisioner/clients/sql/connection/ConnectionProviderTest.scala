package it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection

import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider.{
  ConnectionProvider,
  SQLConnectionProvider
}
import org.scalatest.funsuite.AnyFunSuite

class ConnectionProviderTest extends AnyFunSuite {
  test("impala") {
    assert(ConnectionProvider.impala().isInstanceOf[SQLConnectionProvider])
  }

  test("kerberized impala") {
    assert(ConnectionProvider.kerberizedImpala().isInstanceOf[SQLConnectionProvider])
  }
}
