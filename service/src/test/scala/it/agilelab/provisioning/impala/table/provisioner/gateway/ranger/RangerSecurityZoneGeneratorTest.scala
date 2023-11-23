package it.agilelab.provisioning.impala.table.provisioner.gateway.ranger

import it.agilelab.provisioning.commons.client.ranger.model.{
  RangerSecurityZone,
  RangerSecurityZoneResources
}
import it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.zone.RangerSecurityZoneGenerator
import org.scalatest.funsuite.AnyFunSuite

class RangerSecurityZoneGeneratorTest extends AnyFunSuite {

  test("default RangerSecurityZone") {
    val actual = RangerSecurityZoneGenerator.securityZone(
      "name",
      "serviceName",
      Seq("db_*"),
      Seq("*"),
      Seq("*"),
      Seq("s3a://bucket/folder/*"),
      Seq("adminUser1"),
      Seq("adminUserGroup1"),
      Seq("auditUser1"),
      Seq("auditUserGroup1")
    )
    val expected = RangerSecurityZone(
      -1,
      "name",
      Map(
        "serviceName" -> RangerSecurityZoneResources(
          Seq(
            Map(
              "database" -> Seq("db_*"),
              "column"   -> Seq("*"),
              "table"    -> Seq("*")
            ),
            Map(
              "url" -> Seq("s3a://bucket/folder/*")
            )
          )
        )),
      isEnabled = true,
      Seq("adminUser1"),
      Seq("adminUserGroup1"),
      Seq("auditUser1"),
      Seq("auditUserGroup1")
    )
    assert(actual == expected)
  }

  test("securityZoneWithMergedServiceResources with empty zone") {
    val zone = RangerSecurityZone(
      -1,
      "name",
      Map.empty,
      isEnabled = true,
      Seq("adminUser1"),
      Seq("adminUserGroup1"),
      Seq("auditUser1"),
      Seq("auditUserGroup1")
    )
    val actual = RangerSecurityZoneGenerator.securityZoneWithMergedServiceResources(
      zone,
      "serviceName",
      Seq("db_*"),
      Seq("*"),
      Seq("*"),
      Seq("s3a://bucket/folder/*")
    )
    val expected = RangerSecurityZone(
      -1,
      "name",
      Map(
        "serviceName" -> RangerSecurityZoneResources(
          Seq(
            Map(
              "database" -> Seq("db_*"),
              "column"   -> Seq("*"),
              "table"    -> Seq("*")
            ),
            Map(
              "url" -> Seq("s3a://bucket/folder/*")
            )
          )
        )),
      isEnabled = true,
      Seq("adminUser1"),
      Seq("adminUserGroup1"),
      Seq("auditUser1"),
      Seq("auditUserGroup1")
    )
    assert(actual == expected)
  }

  test("securityZoneWithMergedServiceResources with non empty zone") {
    val zone = RangerSecurityZone(
      -1,
      "name",
      Map(
        "serviceName1" ->
          RangerSecurityZoneResources(
            Seq(
              Map(
                "database" -> Seq("db1_*"),
                "column"   -> Seq("*"),
                "table"    -> Seq("*")
              ),
              Map(
                "url" -> Seq("s3a://bucket/folder1/*")
              )
            )
          )
      ),
      isEnabled = true,
      Seq("adminUser1"),
      Seq("adminUserGroup1"),
      Seq("auditUser1"),
      Seq("auditUserGroup1")
    )
    val actual = RangerSecurityZoneGenerator.securityZoneWithMergedServiceResources(
      zone,
      "serviceName",
      Seq("db_*"),
      Seq("*"),
      Seq("*"),
      Seq("s3a://bucket/folder/*")
    )
    val expected = RangerSecurityZone(
      -1,
      "name",
      Map(
        "serviceName" ->
          RangerSecurityZoneResources(
            Seq(
              Map(
                "database" -> Seq("db_*"),
                "column"   -> Seq("*"),
                "table"    -> Seq("*")
              ),
              Map(
                "url" -> Seq("s3a://bucket/folder/*")
              )
            )
          ),
        "serviceName1" ->
          RangerSecurityZoneResources(
            Seq(
              Map(
                "database" -> Seq("db1_*"),
                "column"   -> Seq("*"),
                "table"    -> Seq("*")
              ),
              Map(
                "url" -> Seq("s3a://bucket/folder1/*")
              )
            )
          )
      ),
      isEnabled = true,
      Seq("adminUser1"),
      Seq("adminUserGroup1"),
      Seq("auditUser1"),
      Seq("auditUserGroup1")
    )
    assert(actual == expected)
  }

  test("securityZoneWithMergedServiceResources merge zone") {
    val zone = RangerSecurityZone(
      -1,
      "name",
      Map(
        "serviceName" ->
          RangerSecurityZoneResources(
            Seq(
              Map(
                "database" -> Seq("db_*"),
                "column"   -> Seq("*"),
                "table"    -> Seq("*")
              ),
              Map(
                "url" -> Seq("s3a://bucket/folder/*")
              )
            )
          )
      ),
      isEnabled = true,
      Seq("adminUser1"),
      Seq("adminUserGroup1"),
      Seq("auditUser1"),
      Seq("auditUserGroup1")
    )
    val actual = RangerSecurityZoneGenerator.securityZoneWithMergedServiceResources(
      zone,
      "serviceName",
      Seq("db_*"),
      Seq("*"),
      Seq("*"),
      Seq("s3a://bucket/folder1/*")
    )
    val expected = RangerSecurityZone(
      -1,
      "name",
      Map(
        "serviceName" ->
          RangerSecurityZoneResources(
            Seq(
              Map(
                "database" -> Seq("db_*"),
                "column"   -> Seq("*"),
                "table"    -> Seq("*")
              ),
              Map(
                "url" -> Seq("s3a://bucket/folder/*", "s3a://bucket/folder1/*")
              )
            )
          )
      ),
      isEnabled = true,
      Seq("adminUser1"),
      Seq("adminUserGroup1"),
      Seq("auditUser1"),
      Seq("auditUserGroup1")
    )
    assert(actual == expected)
  }

  test("securityZoneWithoutUrlResources with empty zone") {
    val zone = RangerSecurityZone(
      -1,
      "name",
      Map.empty,
      isEnabled = true,
      Seq("adminUser1"),
      Seq("adminUserGroup1"),
      Seq("auditUser1"),
      Seq("auditUserGroup1")
    )
    val actual = RangerSecurityZoneGenerator.securityZoneWithoutUrlResources(
      zone,
      "serviceName",
      Seq("db_*"),
      Seq("*"),
      Seq("*"),
      Seq("s3a://bucket/folder/*")
    )
    val expected = RangerSecurityZone(
      -1,
      "name",
      Map(
        "serviceName" -> RangerSecurityZoneResources(
          Seq(
            Map(
              "database" -> Seq("db_*"),
              "column"   -> Seq("*"),
              "table"    -> Seq("*")
            )
          )
        )),
      isEnabled = true,
      Seq("adminUser1"),
      Seq("adminUserGroup1"),
      Seq("auditUser1"),
      Seq("auditUserGroup1")
    )
    assert(actual == expected)
  }

  test("securityZoneWithoutUrlResources with non empty zone") {
    val zone = RangerSecurityZone(
      -1,
      "name",
      Map(
        "serviceName1" ->
          RangerSecurityZoneResources(
            Seq(
              Map(
                "database" -> Seq("db1_*"),
                "column"   -> Seq("*"),
                "table"    -> Seq("*")
              ),
              Map(
                "url" -> Seq("s3a://bucket/folder1/*")
              )
            )
          )
      ),
      isEnabled = true,
      Seq("adminUser1"),
      Seq("adminUserGroup1"),
      Seq("auditUser1"),
      Seq("auditUserGroup1")
    )
    val actual = RangerSecurityZoneGenerator.securityZoneWithoutUrlResources(
      zone,
      "serviceName",
      Seq("db_*"),
      Seq("*"),
      Seq("*"),
      Seq("s3a://bucket/folder/*")
    )
    val expected = RangerSecurityZone(
      -1,
      "name",
      Map(
        "serviceName" ->
          RangerSecurityZoneResources(
            Seq(
              Map(
                "database" -> Seq("db_*"),
                "column"   -> Seq("*"),
                "table"    -> Seq("*")
              )
            )
          ),
        "serviceName1" ->
          RangerSecurityZoneResources(
            Seq(
              Map(
                "database" -> Seq("db1_*"),
                "column"   -> Seq("*"),
                "table"    -> Seq("*")
              ),
              Map(
                "url" -> Seq("s3a://bucket/folder1/*")
              )
            )
          )
      ),
      isEnabled = true,
      Seq("adminUser1"),
      Seq("adminUserGroup1"),
      Seq("auditUser1"),
      Seq("auditUserGroup1")
    )
    assert(actual == expected)
  }

  test("securityZoneWithoutUrlResources merge zone") {
    val zone = RangerSecurityZone(
      -1,
      "name",
      Map(
        "serviceName" ->
          RangerSecurityZoneResources(
            Seq(
              Map(
                "database" -> Seq("db_*"),
                "column"   -> Seq("*"),
                "table"    -> Seq("*")
              ),
              Map(
                "url" -> Seq("s3a://bucket/folder/*", "s3a://bucket/folder1/*")
              )
            )
          )
      ),
      isEnabled = true,
      Seq("adminUser1"),
      Seq("adminUserGroup1"),
      Seq("auditUser1"),
      Seq("auditUserGroup1")
    )
    val actual = RangerSecurityZoneGenerator.securityZoneWithoutUrlResources(
      zone,
      "serviceName",
      Seq("db_*"),
      Seq("*"),
      Seq("*"),
      Seq("s3a://bucket/folder1/*")
    )
    val expected = RangerSecurityZone(
      -1,
      "name",
      Map(
        "serviceName" ->
          RangerSecurityZoneResources(
            Seq(
              Map(
                "database" -> Seq("db_*"),
                "column"   -> Seq("*"),
                "table"    -> Seq("*")
              ),
              Map(
                "url" -> Seq("s3a://bucket/folder/*")
              )
            )
          )
      ),
      isEnabled = true,
      Seq("adminUser1"),
      Seq("adminUserGroup1"),
      Seq("auditUser1"),
      Seq("auditUserGroup1")
    )
    assert(actual == expected)
  }
}
