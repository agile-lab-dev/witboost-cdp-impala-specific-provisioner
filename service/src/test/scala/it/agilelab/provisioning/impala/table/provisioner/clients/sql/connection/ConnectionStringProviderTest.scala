package it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection

import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider.{
  ConnectionConfig,
  ConnectionStringProvider
}
import org.scalatest.funsuite.AnyFunSuite

class ConnectionStringProviderTest extends AnyFunSuite {

  test("get") {
    val actual = new ConnectionStringProvider("jdbc:xx://%s:%s/%s;USER=%s;PWD=%s").get(
      ConnectionConfig("hst1", "p22", "test", "usr", "pwd"))

    val expected = "jdbc:xx://hst1:p22/test;USER=usr;PWD=pwd"

    assert(actual == expected)
  }

}
