package it.agilelab.provisioning.impala.table.provisioner.gateway.table

import org.scalatest.funsuite.AnyFunSuite

class ExternalTableGatewayTest extends AnyFunSuite {

  test("impala") {
    val actual = ExternalTableGateway.impala("usr", "pwd")
    assert(actual.isInstanceOf[ImpalaExternalTableGateway])
  }

  test("impala with audit") {
    val actual = ExternalTableGateway.impalaWithAudit("usr", "pwd")
    assert(actual.isInstanceOf[ImpalaExternalTableGatewayWithAudit])
  }
}
