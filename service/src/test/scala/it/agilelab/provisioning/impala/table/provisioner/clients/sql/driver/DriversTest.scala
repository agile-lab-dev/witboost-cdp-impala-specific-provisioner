package it.agilelab.provisioning.impala.table.provisioner.clients.sql.driver

import org.scalatest.funsuite.AnyFunSuite

class DriversTest extends AnyFunSuite {

  test("impala") {
    assert(Drivers.impala == "com.cloudera.impala.jdbc.Driver")
  }
}
