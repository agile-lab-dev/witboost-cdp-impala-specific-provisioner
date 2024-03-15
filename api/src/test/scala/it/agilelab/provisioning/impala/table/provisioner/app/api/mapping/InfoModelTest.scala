package it.agilelab.provisioning.impala.table.provisioner.app.api.mapping

import org.scalatest.funsuite.AnyFunSuite

class InfoModelTest extends AnyFunSuite {

  test("create a string info") {
    val expected = InfoModel("string", "label", "value", Some("http://example.com"))

    assert(expected == InfoModel.makeStringInfoObject("label", "value", Some("http://example.com")))
  }
}
