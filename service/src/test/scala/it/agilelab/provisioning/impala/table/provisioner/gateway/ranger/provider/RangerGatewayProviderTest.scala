package it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.provider

import org.scalatest.funsuite.AnyFunSuite

class RangerGatewayProviderTest extends AnyFunSuite {

  test("basic auth") {
    val provider = new RangerGatewayProvider("username", "password")

    val rangerClient = provider.getRangerClient("https://ranger.client/ranger/")
    assert(rangerClient.isRight)
  }

}
