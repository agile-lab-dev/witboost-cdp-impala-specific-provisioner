package it.agilelab.provisioning.impala.table.provisioner.gateway.view

import it.agilelab.provisioning.impala.table.provisioner.gateway.table.ImpalaExternalTableGatewayWithAudit
import org.scalatest.funsuite.AnyFunSuite

class ViewGatewayTest extends AnyFunSuite {

  test("impala") {
    val actual = ViewGateway.impala("usr", "pwd")
    assert(actual.isInstanceOf[ImpalaViewGateway])
  }

  test("impala with audit") {
    val actual = ViewGateway.impalaWithAudit("usr", "pwd")
    assert(actual.isInstanceOf[ImpalaViewGatewayWithAudit])
  }

  test("kerberized impala") {
    val actual = ViewGateway.kerberizedImpala()
    assert(actual.isInstanceOf[ImpalaViewGateway])
  }

  test("kerberized impala with audit") {
    val actual = ViewGateway.kerberizedImpalaWithAudit()
    assert(actual.isInstanceOf[ImpalaViewGatewayWithAudit])
  }
}
