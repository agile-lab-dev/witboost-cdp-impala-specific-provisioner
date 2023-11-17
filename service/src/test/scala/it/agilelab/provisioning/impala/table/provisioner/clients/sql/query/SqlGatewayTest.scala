package it.agilelab.provisioning.impala.table.provisioner.clients.sql.query

import org.scalatest.funsuite.AnyFunSuite

class SqlGatewayTest extends AnyFunSuite {

  test("default") {
    val actual = SqlGateway.impala()
    assert(actual.isInstanceOf[DefaultSQLGateway])
  }
}
