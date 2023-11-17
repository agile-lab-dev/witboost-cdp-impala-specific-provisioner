package it.agilelab.provisioning.impala.table.provisioner.gateway.ranger

import it.agilelab.provisioning.commons.client.ranger.model.RangerResource
import org.scalatest.funsuite.AnyFunSuite

class RangerResourcesTest extends AnyFunSuite {

  test("database") {
    val actual = RangerResources.database("db")
    val expected = Map(
      "database" -> RangerResource(Seq("db"), isExcludes = false, isRecursive = false)
    )
    assert(actual == expected)
  }

  test("table") {
    val actual = RangerResources.table("db", "tbl")
    val expected = Map(
      "database" -> RangerResource(Seq("db"), isExcludes = false, isRecursive = false),
      "table"    -> RangerResource(Seq("tbl"), isExcludes = false, isRecursive = false),
      "column"   -> RangerResource(Seq("*"), isExcludes = false, isRecursive = false)
    )
    assert(actual == expected)
  }

  test("tableWithDbResourceExcluded") {
    val actual = RangerResources.tableWithDbResourceExcluded("db", "tbl")
    val expected = Map(
      "database" -> RangerResource(Seq("db"), isExcludes = true, isRecursive = false),
      "table"    -> RangerResource(Seq("tbl"), isExcludes = false, isRecursive = false),
      "column"   -> RangerResource(Seq("*"), isExcludes = false, isRecursive = false)
    )
    assert(actual == expected)
  }

  test("url") {
    val actual = RangerResources.url("url")
    val expected = Map(
      "url" -> RangerResource(Seq("url"), isExcludes = false, isRecursive = true)
    )
    assert(actual == expected)
  }
}
