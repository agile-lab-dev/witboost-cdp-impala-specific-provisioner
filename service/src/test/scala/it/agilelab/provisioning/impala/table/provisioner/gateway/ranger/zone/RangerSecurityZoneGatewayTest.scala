package it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.zone

import it.agilelab.provisioning.commons.client.ranger.RangerClient
import it.agilelab.provisioning.commons.client.ranger.RangerClientError.{
  CreateSecurityZoneErr,
  FindSecurityZoneByNameErr,
  UpdateSecurityZoneErr
}
import it.agilelab.provisioning.commons.client.ranger.model.{
  RangerSecurityZone,
  RangerSecurityZoneResources,
  RangerService
}
import it.agilelab.provisioning.commons.http.HttpErrors.ConnectionErr
import it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.RangerGatewayTestSupport
import it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.zone.RangerSecurityZoneGatewayError.FindServiceErr
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

class RangerSecurityZoneGatewayTest
    extends AnyFunSuite
    with MockFactory
    with RangerGatewayTestSupport {
  val client: RangerClient = stub[RangerClient]

  val gateway =
    new RangerSecurityZoneGateway(client)
  val zone: RangerSecurityZone = RangerSecurityZone(
    1,
    "domain_dpName_0",
    Map(
      "nifi" -> RangerSecurityZoneResources(
        Seq(
          Map(
            "nifi-resource" -> Seq("res1", "res2")
          )
        )
      )
    ),
    isEnabled = true,
    List("adminUser1", "adminUser2"),
    List("adminUserGroup1", "adminUserGroup2"),
    List("auditUser1", "auditUser2"),
    List("auditUserGroup1", "auditUserGroup2")
  )
  val zoneUpdated: RangerSecurityZone = RangerSecurityZone(
    1,
    "domain_dpName_0",
    Map(
      "cm_hive" -> RangerSecurityZoneResources(
        Seq(
          Map(
            "database" -> Seq("domain_dpName_0_*"),
            "column"   -> Seq("*"),
            "table"    -> Seq("*")
          ),
          Map(
            "url" -> Seq("url1/*", "url2/*")
          )
        )
      ),
      "nifi" -> RangerSecurityZoneResources(
        Seq(
          Map(
            "nifi-resource" -> Seq("res1", "res2")
          )
        )
      )
    ),
    isEnabled = true,
    List("adminUser1", "adminUser2"),
    List("adminUserGroup1", "adminUserGroup2"),
    List("auditUser1", "auditUser2"),
    List("auditUserGroup1", "auditUserGroup2")
  )
  val hiveService: RangerService = RangerService(
    1,
    isEnabled = true,
    "hive",
    "cm_hive",
    "Hadoop SQL",
    Map(
      "cluster.name"                  -> "cdpDlName",
      "tag.download.auth.users"       -> "hive,hdfs,impala",
      "password"                      -> "*****",
      "policy.download.auth.users"    -> "hive,hdfs,impala",
      "policy.grantrevoke.auth.users" -> "hive,impala",
      "enable.hive.metastore.lookup"  -> "true",
      "default.policy.users"          -> "impala,beacon,hue,admin,dpprofiler",
      "ranger.plugin.audit.filters"   -> "filter",
      "jdbc.driverClassName"          -> "org.apache.hive.jdbc.HiveDriver",
      "hive.site.file.path"           -> "hive-site.xml",
      "jdbc.url"                      -> "none",
      "username"                      -> "hive"
    )
  )

  test("upsertSecurityZone return Right(RangerSecurityZone) when security zone already exists") {

    (client.findSecurityZoneByName _)
      .when("domain_dpName_0")
      .returns(Right(Some(zone)))

    (client.findAllServices _)
      .when()
      .returns(
        Right(
          List(
            hiveService,
            RangerService(1, isEnabled = true, "nifi", "cm_nifi", "NiFi", Map.empty))))

    (client.updateSecurityZone _)
      .when(zoneUpdated)
      .returns(Right(zoneUpdated))

    val actual = gateway.upsertSecurityZone(
      "srvRole",
      "domain_dpName_0",
      "dataProductOwner",
      Some("devGroup"),
      "hive",
      "cdpDlName",
      Seq("url1", "url2"))
    val expected = Right(zoneUpdated)
    assert(actual == expected)
  }

  test(
    "upsertSecurityZone return Right(RangerSecurityZone) when security zone already exists with isDestroy") {
    (client.findSecurityZoneByName _)
      .when("domain_dpName_0")
      .returns(
        Right(
          Some(
            RangerSecurityZone(
              1,
              "domain_dpName_0",
              Map(
                "cm_hive" -> RangerSecurityZoneResources(
                  Seq(
                    Map(
                      "database" -> Seq("domain_dpName_0_*"),
                      "column"   -> Seq("*"),
                      "table"    -> Seq("*")
                    ),
                    Map(
                      "url" -> Seq("url1/*")
                    )
                  )
                ),
                "nifi" -> RangerSecurityZoneResources(
                  Seq(
                    Map(
                      "nifi-resource" -> Seq("res1", "res2")
                    )
                  )
                )
              ),
              isEnabled = true,
              List("adminUser1", "adminUser2"),
              List("adminUserGroup1", "adminUserGroup2"),
              List("auditUser1", "auditUser2"),
              List("auditUserGroup1", "auditUserGroup2")
            )
          )))

    (client.findAllServices _)
      .when()
      .returns(
        Right(
          List(
            hiveService,
            RangerService(1, isEnabled = true, "nifi", "cm_nifi", "NiFi", Map.empty))))

    (client.updateSecurityZone _)
      .when(RangerSecurityZone(
        1,
        "domain_dpName_0",
        Map(
          "cm_hive" -> RangerSecurityZoneResources(
            Seq(
              Map(
                "database" -> Seq("domain_dpName_0_*"),
                "column"   -> Seq("*"),
                "table"    -> Seq("*")
              )
            )
          ),
          "nifi" -> RangerSecurityZoneResources(
            Seq(
              Map(
                "nifi-resource" -> Seq("res1", "res2")
              )
            )
          )
        ),
        isEnabled = true,
        List("adminUser1", "adminUser2"),
        List("adminUserGroup1", "adminUserGroup2"),
        List("auditUser1", "auditUser2"),
        List("auditUserGroup1", "auditUserGroup2")
      ))
      .returns(Right(RangerSecurityZone(
        1,
        "domain_dpName_0",
        Map(
          "cm_hive" -> RangerSecurityZoneResources(
            Seq(
              Map(
                "database" -> Seq("domain_dpName_0_*"),
                "column"   -> Seq("*"),
                "table"    -> Seq("*")
              )
            )
          ),
          "nifi" -> RangerSecurityZoneResources(
            Seq(
              Map(
                "nifi-resource" -> Seq("res1", "res2")
              )
            )
          )
        ),
        isEnabled = true,
        List("adminUser1", "adminUser2"),
        List("adminUserGroup1", "adminUserGroup2"),
        List("auditUser1", "auditUser2"),
        List("auditUserGroup1", "auditUserGroup2")
      )))

    val actual =
      gateway.upsertSecurityZone(
        "srvRole",
        "domain_dpName_0",
        "dataProductOwner",
        Some("devGroup"),
        "hive",
        "cdpDlName",
        Seq("url1"),
        isDestroy = true)

    val expected = Right(
      RangerSecurityZone(
        1,
        "domain_dpName_0",
        Map(
          "cm_hive" -> RangerSecurityZoneResources(
            Seq(
              Map(
                "database" -> Seq("domain_dpName_0_*"),
                "column"   -> Seq("*"),
                "table"    -> Seq("*")
              )
            )
          ),
          "nifi" -> RangerSecurityZoneResources(
            Seq(
              Map(
                "nifi-resource" -> Seq("res1", "res2")
              )
            )
          )
        ),
        isEnabled = true,
        List("adminUser1", "adminUser2"),
        List("adminUserGroup1", "adminUserGroup2"),
        List("auditUser1", "auditUser2"),
        List("auditUserGroup1", "auditUserGroup2")
      ))
    assert(actual == expected)
  }

  test("upsertSecurityZone return Right(RangerSecurityZone) when security zone does not exists") {
    val newZone = RangerSecurityZone(
      -1,
      "domain_dpName_0",
      Map(
        "cm_hive" ->
          RangerSecurityZoneResources(
            Seq(
              Map(
                "database" -> Seq("domain_dpName_0_*"),
                "column"   -> Seq("*"),
                "table"    -> Seq("*")
              ),
              Map(
                "url" -> Seq("url1/*", "url2/*")
              )
            )
          )
      ),
      isEnabled = true,
      Seq("srvRole"),
      Seq.empty,
      Seq("srvRole", "dataProductOwner"),
      Seq("devGroup")
    )
    (client.findSecurityZoneByName _)
      .when("domain_dpName_0")
      .returns(Right(None))

    (client.findAllServices _)
      .when()
      .returns(
        Right(
          List(
            hiveService,
            RangerService(1, isEnabled = true, "nifi", "cm_nifi", "NiFi", Map.empty))))

    (client.createSecurityZone _)
      .when(newZone)
      .returns(Right(newZone))

    val actual = gateway.upsertSecurityZone(
      "srvRole",
      "domain_dpName_0",
      "dataProductOwner",
      Some("devGroup"),
      "hive",
      "cdpDlName",
      Seq("url1", "url2"))
    val expected = Right(newZone)
    assert(actual == expected)
  }

  test(
    "upsertSecurityZone return Right(RangerSecurityZone) when security zone does not exists with isDestroy") {
    val newZone = RangerSecurityZone(
      -1,
      "domain_dpName_0",
      Map(
        "cm_hive" ->
          RangerSecurityZoneResources(
            Seq(
              Map(
                "database" -> Seq("domain_dpName_0_*"),
                "column"   -> Seq("*"),
                "table"    -> Seq("*")
              )
            )
          )
      ),
      isEnabled = true,
      Seq("srvRole"),
      Seq.empty,
      Seq("srvRole", "dataProductOwner"),
      Seq("devGroup")
    )
    (client.findSecurityZoneByName _)
      .when("domain_dpName_0")
      .returns(Right(None))

    (client.findAllServices _)
      .when()
      .returns(
        Right(
          List(
            hiveService,
            RangerService(1, isEnabled = true, "nifi", "cm_nifi", "NiFi", Map.empty))))

    (client.createSecurityZone _)
      .when(newZone)
      .returns(Right(newZone))

    val actual = gateway.upsertSecurityZone(
      "srvRole",
      "domain_dpName_0",
      "dataProductOwner",
      Some("devGroup"),
      "hive",
      "cdpDlName",
      Seq("url1", "url2"),
      isDestroy = true)
    val expected = Right(newZone)
    assert(actual == expected)
  }

  test(
    "upsertSecurityZone return Left(UpsertSecurityZoneErr(FindSecurityZoneByNameErr)) when security zone is not available") {
    (client.findSecurityZoneByName _)
      .when("domain_dpName_0")
      .returns(
        Left(
          FindSecurityZoneByNameErr(
            "domain_dpName_0",
            ConnectionErr("xx", new IllegalArgumentException("x")))
        ))

    val actual = gateway.upsertSecurityZone(
      "srvRole",
      "domain_dpName_0",
      "dataProductOwner",
      Some("devGroup"),
      "hive",
      "cdpDlName",
      Seq("url1", "url2"))

    assertUpsertZoneWithFindZoneErr(
      actual,
      "domain_dpName_0",
      ConnectionErr("xx", new IllegalArgumentException("x")))
  }

  test("upsertSecurityZone return Left(FindServiceErr) when service is not available") {
    (client.findSecurityZoneByName _)
      .when("domain_dpName_0")
      .returns(Right(Some(zone)))

    (client.findAllServices _)
      .when()
      .returns(Right(List.empty))

    val actual = gateway.upsertSecurityZone(
      "srvRole",
      "domain_dpName_0",
      "dataProductOwner",
      Some("devGroup"),
      "hive",
      "cdpDlName",
      Seq("url1", "url2"))
    val expected = Left(
      FindServiceErr("Unable to find service with type hive in cluster cdpDlName.")
    )
    assert(actual == expected)
  }

  test("upsertSecurityZone return Left(UpsertSecurityZoneErr) when update fails") {
    (client.findSecurityZoneByName _)
      .when("domain_dpName_0")
      .returns(Right(Some(zone)))

    (client.findAllServices _)
      .when()
      .returns(
        Right(
          List(
            hiveService,
            RangerService(1, isEnabled = true, "nifi", "cm_nifi", "NiFi", Map.empty))))

    (client.updateSecurityZone _)
      .when(zoneUpdated)
      .returns(
        Left(
          UpdateSecurityZoneErr(zone, ConnectionErr("xx", new IllegalArgumentException("x")))
        ))

    val actual = gateway.upsertSecurityZone(
      "srvRole",
      "domain_dpName_0",
      "dataProductOwner",
      Some("devGroup"),
      "hive",
      "cdpDlName",
      Seq("url1", "url2"))

    assertUpsertZoneWithUpdateZoneErr(
      actual,
      zone,
      ConnectionErr("xx", new IllegalArgumentException("x")))
  }

  test("upsertSecurityZone return Left(UpsertSecurityZoneErr) when create fails") {
    val zoneToBeCreated = RangerSecurityZone(
      -1,
      "domain_dpName_0",
      Map(
        "cm_hive" ->
          RangerSecurityZoneResources(
            Seq(
              Map(
                "database" -> Seq("domain_dpName_0_*"),
                "column"   -> Seq("*"),
                "table"    -> Seq("*")
              ),
              Map(
                "url" -> Seq("url1/*", "url2/*")
              )
            )
          )
      ),
      isEnabled = true,
      Seq("srvRole"),
      Seq.empty,
      Seq("srvRole", "dataProductOwner"),
      Seq("devGroup")
    )
    (client.findSecurityZoneByName _)
      .when("domain_dpName_0")
      .returns(Right(None))

    (client.findAllServices _)
      .when()
      .returns(
        Right(
          List(
            hiveService,
            RangerService(1, isEnabled = true, "nifi", "cm_nifi", "NiFi", Map.empty))))

    (client.createSecurityZone _)
      .when(zoneToBeCreated)
      .returns(
        Left(
          CreateSecurityZoneErr(
            zoneToBeCreated,
            ConnectionErr("xx", new IllegalArgumentException("x")))
        ))

    val actual = gateway.upsertSecurityZone(
      "srvRole",
      "domain_dpName_0",
      "dataProductOwner",
      Some("devGroup"),
      "hive",
      "cdpDlName",
      Seq("url1", "url2"))

    assertUpsertZoneWithCreateZoneErr(
      actual,
      zoneToBeCreated,
      ConnectionErr("xx", new IllegalArgumentException("x")))
  }

}
