package it.agilelab.provisioning.impala.table.provisioner.clients.sql.query

import org.scalatest.funsuite.AnyFunSuite

class SqlGatewayTest extends AnyFunSuite {

  test("impala") {
    val actual = SqlGateway.impala()
    assert(actual.isInstanceOf[DefaultSQLGateway])
  }

  test("kerberized impala") {
    val actual = SqlGateway.kerberizedImpala()
    assert(actual.isInstanceOf[DefaultSQLGateway])
  }
}
