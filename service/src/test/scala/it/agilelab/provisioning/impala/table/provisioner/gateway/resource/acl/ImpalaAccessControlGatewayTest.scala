package it.agilelab.provisioning.impala.table.provisioner.gateway.resource.acl

import cats.implicits.showInterpolator
import com.cloudera.cdp.datalake.model.Datalake
import io.circe.Json
import io.circe.generic.auto.exportEncoder
import io.circe.syntax.EncoderOps
import it.agilelab.provisioning.commons.client.ranger.RangerClient
import it.agilelab.provisioning.commons.client.ranger.RangerClientError.UpdateRoleErr
import it.agilelab.provisioning.commons.client.ranger.model.{
  RangerRole,
  RangerSecurityZone,
  RangerSecurityZoneResources,
  RoleMember
}
import it.agilelab.provisioning.commons.http.HttpErrors
import it.agilelab.provisioning.commons.principalsmapping.{
  CdpIamGroup,
  CdpIamPrincipals,
  CdpIamUser,
  PrincipalsMapper
}
import it.agilelab.provisioning.impala.table.provisioner.clients.cdp.CDPPublicHostProvider
import it.agilelab.provisioning.impala.table.provisioner.common.{
  OutputPortFaker,
  ProvisionRequestFaker,
  StorageAreaFaker
}
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaFormat.Csv
import it.agilelab.provisioning.impala.table.provisioner.core.model._
import it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.policy.RangerPolicyGateway
import it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.provider.{
  RangerGateway,
  RangerGatewayProvider
}
import it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.role.RangerRoleGatewayError.UpsertRoleErr
import it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.role.{
  OwnerRoleType,
  RangerRoleGateway,
  UserRoleType
}
import it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.zone.RangerSecurityZoneGateway
import it.agilelab.provisioning.mesh.self.service.api.model.openmetadata.{ Column, ColumnDataType }
import it.agilelab.provisioning.mesh.self.service.core.gateway.ComponentGatewayError
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

class ImpalaAccessControlGatewayTest extends AnyFunSuite with MockFactory {

  test("provision access control for simple table") {

    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        OutputPortFaker(
          PublicImpalaCdw(
            databaseName = "databaseName",
            tableName = "tableName",
            cdpEnvironment = "cdpEnv",
            cdwVirtualWarehouse = "service",
            format = Csv,
            location = "loc",
            partitions = None
          ).asJson).build()
      )
      .build()

    val datalake = new Datalake()
    datalake.setEnvironmentCrn("cdpEnvCrn")
    datalake.setDatalakeName("dlName")

    val externalTable = ExternalTable(
      "databaseName",
      "tableName",
      Seq(Field("id", ImpalaDataType.ImpalaInt, None)),
      Seq.empty,
      "loc",
      Csv)

    val hostProvider = stub[CDPPublicHostProvider]
    (hostProvider.getRangerHost _)
      .when(*)
      .returns(Right("rangerHost"))

    val policyGateway = stub[RangerPolicyGateway]
    (policyGateway.upsertPolicies _)
      .when(*, *, *, *, *, *, *)
      .returns(
        Right(
          Seq(
            PolicyAttachment("123", "xy"),
            PolicyAttachment("456", "ttt"),
            PolicyAttachment("789", "loc")
          ))
      )

    val rangerRoleGateway = stub[RangerRoleGateway]
    (rangerRoleGateway.upsertRole _)
      .when("domain_dp-name_0", OwnerRoleType, *, *, *, *, *)
      .returns(
        Right(
          RangerRole(
            id = 1,
            isEnabled = true,
            name = "domain_dp_name_0_owner",
            description = "",
            groups = Seq(RoleMember("devGroup", isAdmin = false)),
            users = Seq(
              RoleMember("srvRole", isAdmin = true),
              RoleMember("dataProductOwner", isAdmin = false)),
            roles = Seq.empty
          )
        )
      )

    (rangerRoleGateway.upsertRole _)
      .when("domain_dp-name_0_cmp-name", UserRoleType, *, *, *, *, *)
      .returns(
        Right(
          RangerRole(
            id = 1,
            isEnabled = true,
            name = "domain_dp_name_0_cmp_name_read",
            description = "",
            groups = Seq.empty,
            users = Seq(
              RoleMember("srvRole", isAdmin = true)
            ),
            roles = Seq.empty
          )
        )
      )

    val securityZoneGateway = stub[RangerSecurityZoneGateway]
    (securityZoneGateway.upsertSecurityZone _)
      .when(
        "srvRole",
        "domain_dp_name_0",
        "dataProductOwner",
        Some("devGroup"),
        "hive",
        "dlName",
        Seq("loc"),
        false)
      .returns(
        Right(
          RangerSecurityZone(
            1,
            "domain_dp_name_0",
            Map(
              "cm_hive" -> RangerSecurityZoneResources(
                Seq(
                  Map(
                    "database" -> Seq("domain_*"),
                    "column"   -> Seq("*"),
                    "table"    -> Seq("*")
                  ),
                  Map(
                    "url" -> Seq("loc/*")
                  )
                )
              )
            ),
            isEnabled = true,
            List("srvRole", "dataProductOwner"),
            List("devGroup"),
            List("srvRole", "dataProductOwner"),
            List("devGroup")
          )
        )
      )

    val rangerClient = stub[RangerClient]
    val rangerGatewayProvider = stub[RangerGatewayProvider]
    (rangerGatewayProvider.getRangerGatewaysFromClient _)
      .when(*)
      .returns(Right(new RangerGateway(policyGateway, securityZoneGateway, rangerRoleGateway)))

    val principalsMapper = stub[PrincipalsMapper[CdpIamPrincipals]]
    (principalsMapper.map _)
      .when(Set("dataProductOwner", "devGroup"))
      .returns(
        Map(
          "dataProductOwner" -> Right(CdpIamUser("", "dataProductOwner", "")),
          "devGroup"         -> Right(CdpIamGroup("devGroup", ""))
        ))

    val expected = Right(
      Seq(
        PolicyAttachment("123", "xy"),
        PolicyAttachment("456", "ttt"),
        PolicyAttachment("789", "loc")
      ))

    val impalaOutputPortAccessControlGateway = new ImpalaAccessControlGateway(
      serviceRole = "srvRole",
      rangerGatewayProvider = rangerGatewayProvider,
      principalsMapper = principalsMapper
    )

    val accessControlInfo = AccessControlInfo(
      dataProductOwner = "dataProductOwner",
      devGroup = "devGroup",
      componentId = "urn:dmb:cmp:domain:dp-name:0:cmp-name"
    )

    val actual =
      impalaOutputPortAccessControlGateway.provisionAccessControl(
        accessControlInfo,
        rangerClient,
        externalTable,
        "dlName",
        true)

    assert(actual == expected)
  }

  test("provision access control should fail on wrong id") {

    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        OutputPortFaker(
          PublicImpalaCdw(
            databaseName = "databaseName",
            tableName = "tableName",
            cdpEnvironment = "cdpEnv",
            cdwVirtualWarehouse = "service",
            format = Csv,
            location = "loc",
            partitions = None
          ).asJson)
          .withId("urn:dmb:dp:domain:dp-name:0")
          .build()
      )
      .build()

    val externalTable = ExternalTable(
      "databaseName",
      "tableName",
      Seq(Field("id", ImpalaDataType.ImpalaInt, None)),
      Seq.empty,
      "loc",
      Csv)

    val rangerClient = stub[RangerClient]
    val rangerGatewayProvider = stub[RangerGatewayProvider]
    val principalsMapper = stub[PrincipalsMapper[CdpIamPrincipals]]

    val expected = Left(
      ComponentGatewayError("Component id is not in the expected shape, cannot extract attributes"))

    val impalaOutputPortAccessControlGateway = new ImpalaAccessControlGateway(
      serviceRole = "srvRole",
      rangerGatewayProvider = rangerGatewayProvider,
      principalsMapper = principalsMapper
    )

    val accessControlInfo = AccessControlInfo(
      dataProductOwner = "dataProductOwner",
      devGroup = "devGroup",
      componentId = "urn:dmb:dp:domain:dp-name:0"
    )

    val actual =
      impalaOutputPortAccessControlGateway.provisionAccessControl(
        accessControlInfo,
        rangerClient,
        externalTable,
        "dlName",
        true)

    assert(actual == expected)
  }

  test("unprovision access control") {

    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        OutputPortFaker[Json](
          PublicImpalaCdw(
            databaseName = "databaseName",
            tableName = "tableName",
            cdpEnvironment = "cdpEnv",
            cdwVirtualWarehouse = "service",
            format = Csv,
            location = "loc",
            partitions = None
          ).asJson).build()
      )
      .build()

    val externalTable = ExternalTable(
      "databaseName",
      "tableName",
      Seq(Field("id", ImpalaDataType.ImpalaInt, None)),
      Seq.empty,
      "loc",
      Csv)

    val hostProvider = stub[CDPPublicHostProvider]
    (hostProvider.getRangerHost _)
      .when(*)
      .returns(Right("rangerHost"))

    val policyGateway = stub[RangerPolicyGateway]
    (policyGateway.deletePolicies _)
      .when(*, *, *, *, *)
      .returns(
        Right(
          Seq(
            PolicyAttachment("456", "ttt"),
            PolicyAttachment("789", "loc")
          ))
      )

    val securityZoneGateway = stub[RangerSecurityZoneGateway]
    (securityZoneGateway.upsertSecurityZone _)
      .when(
        "srvRole",
        "domain_dp_name_0",
        "dataProductOwner",
        Some("devGroup"),
        "hive",
        "dlName",
        Seq("loc"),
        true)
      .returns(
        Right(
          RangerSecurityZone(
            1,
            "domain_dp_name_0",
            Map(
              "cm_hive" -> RangerSecurityZoneResources(
                Seq(
                  Map(
                    "database" -> Seq("domain_*"),
                    "column"   -> Seq("*"),
                    "table"    -> Seq("*")
                  ),
                  Map(
                    "url" -> Seq.empty[String]
                  )
                )
              )
            ),
            isEnabled = true,
            List("srvRole", "dataProductOwner"),
            List("devGroup"),
            List("srvRole", "dataProductOwner"),
            List("devGroup")
          )
        )
      )

    val rangerRoleGateway = stub[RangerRoleGateway]
    (rangerRoleGateway.deleteUserRole _)
      .when(*)
      .returns(
        Right(
          RangerRole(
            id = 1,
            isEnabled = true,
            name = "domain_dp_name_0_read",
            description = "",
            groups = Seq.empty,
            users = Seq(
              RoleMember("srvRole", isAdmin = true),
              RoleMember("user1", isAdmin = false)
            ),
            roles = Seq.empty
          )
        )
      )

    val rangerGatewayProvider = stub[RangerGatewayProvider]
    (rangerGatewayProvider.getRangerGatewaysFromClient _)
      .when(*)
      .returns(Right(new RangerGateway(policyGateway, securityZoneGateway, rangerRoleGateway)))

    val rangerClient = stub[RangerClient]

    val principalsMapper = stub[PrincipalsMapper[CdpIamPrincipals]]
    (principalsMapper.map _)
      .when(Set("dataProductOwner", "devGroup"))
      .returns(
        Map(
          "dataProductOwner" -> Right(CdpIamUser("", "dataProductOwner", "")),
          "devGroup"         -> Right(CdpIamGroup("devGroup", ""))
        ))

    val expected = Right(
      Seq(
        PolicyAttachment("456", "ttt"),
        PolicyAttachment("789", "loc")
      ))

    val impalaOutputPortAccessControlGateway = new ImpalaAccessControlGateway(
      serviceRole = "srvRole",
      rangerGatewayProvider = rangerGatewayProvider,
      principalsMapper = principalsMapper
    )

    val accessControlInfo = AccessControlInfo(
      dataProductOwner = "dataProductOwner",
      devGroup = "devGroup",
      componentId = "urn:dmb:cmp:domain:dp-name:0:cmp-name"
    )

    val actual =
      impalaOutputPortAccessControlGateway.unprovisionAccessControl(
        accessControlInfo,
        rangerClient,
        externalTable,
        "dlName",
        true)

    assert(actual == expected)
  }

  test("unprovision access control should fail on wrong id") {

    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        OutputPortFaker(
          PublicImpalaCdw(
            databaseName = "databaseName",
            tableName = "tableName",
            cdpEnvironment = "cdpEnv",
            cdwVirtualWarehouse = "service",
            format = Csv,
            location = "loc",
            partitions = None
          ).asJson)
          .withId("urn:dmb:dp:domain:dp-name:0")
          .build()
      )
      .build()

    val datalake = new Datalake()
    datalake.setEnvironmentCrn("cdpEnvCrn")
    datalake.setDatalakeName("dlName")

    val externalTable = ExternalTable(
      "databaseName",
      "tableName",
      Seq(Field("id", ImpalaDataType.ImpalaInt, None)),
      Seq.empty,
      "loc",
      Csv)

    val rangerClient = stub[RangerClient]
    val rangerGatewayProvider = stub[RangerGatewayProvider]
    val principalsMapper = stub[PrincipalsMapper[CdpIamPrincipals]]

    val expected = Left(
      ComponentGatewayError("Component id is not in the expected shape, cannot extract attributes"))

    val impalaOutputPortAccessControlGateway = new ImpalaAccessControlGateway(
      serviceRole = "srvRole",
      rangerGatewayProvider = rangerGatewayProvider,
      principalsMapper = principalsMapper
    )

    val accessControlInfo = AccessControlInfo(
      dataProductOwner = "dataProductOwner",
      devGroup = "devGroup",
      componentId = "urn:dmb:dp:domain:dp-name:0"
    )

    val actual =
      impalaOutputPortAccessControlGateway.unprovisionAccessControl(
        accessControlInfo,
        rangerClient,
        externalTable,
        "dlName",
        true)

    assert(actual == expected)
  }

  test("update acl") {
    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        OutputPortFaker(
          PublicImpalaCdw(
            databaseName = "databaseName",
            tableName = "tableName",
            cdpEnvironment = "cdpEnv",
            cdwVirtualWarehouse = "service",
            format = Csv,
            location = "loc",
            partitions = None
          ).asJson).build()
      )
      .build()

    val datalake = new Datalake()
    datalake.setEnvironmentCrn("cdpEnvCrn")
    datalake.setDatalakeName("dlName")

    val userRole = RangerRole(
      id = 1,
      isEnabled = true,
      name = "domain_dp_name_0_cmp_name_read",
      description = "",
      groups = List(RoleMember("group1", isAdmin = false)),
      users = List(
        RoleMember("user1", isAdmin = false),
        RoleMember("user2", isAdmin = false),
        RoleMember("srvRole", isAdmin = true)),
      roles = Seq.empty
    )

    val refs: Set[CdpIamPrincipals] = Set(
      CdpIamUser("", "user1", ""),
      CdpIamUser("", "user2", ""),
      CdpIamGroup("group1", "")
    )

    val hostProvider = stub[CDPPublicHostProvider]
    (hostProvider.getRangerHost _)
      .when(*)
      .returns(Right("rangerHost"))

    val rangerRoleGateway = stub[RangerRoleGateway]
    (rangerRoleGateway.upsertRole _)
      .when(
        "domain_dp-name_0_cmp-name",
        UserRoleType,
        *,
        *,
        *,
        List("user1", "user2"),
        List("group1"))
      .returns(Right(userRole))

    val rangerClient = stub[RangerClient]
    val policyGateway = stub[RangerPolicyGateway]
    val securityZoneGateway = stub[RangerSecurityZoneGateway]
    val rangerGatewayProvider = stub[RangerGatewayProvider]
    (rangerGatewayProvider.getRangerGatewaysFromClient _)
      .when(*)
      .returns(Right(new RangerGateway(policyGateway, securityZoneGateway, rangerRoleGateway)))

    val expected = Right(userRole)

    val principalsMapper = stub[PrincipalsMapper[CdpIamPrincipals]]
    val impalaOutputPortAccessControlGateway = new ImpalaAccessControlGateway(
      serviceRole = "srvRole",
      rangerGatewayProvider = rangerGatewayProvider,
      principalsMapper = principalsMapper
    )

    val accessControlInfo = AccessControlInfo(
      dataProductOwner = "dataProductOwner",
      devGroup = "devGroup",
      componentId = "urn:dmb:cmp:domain:dp-name:0:cmp-name"
    )

    val actual =
      impalaOutputPortAccessControlGateway.updateAcl(accessControlInfo, refs, rangerClient)

    assert(actual == expected)
  }

  test("update acl returns error on upsert error") {
    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        OutputPortFaker(
          PublicImpalaCdw(
            databaseName = "databaseName",
            tableName = "tableName",
            cdpEnvironment = "cdpEnv",
            cdwVirtualWarehouse = "service",
            format = Csv,
            location = "loc",
            partitions = None
          ).asJson).build()
      )
      .build()

    val datalake = new Datalake()
    datalake.setEnvironmentCrn("cdpEnvCrn")
    datalake.setDatalakeName("dlName")

    val userRole = RangerRole(
      id = 1,
      isEnabled = true,
      name = "domain_dp_name_0_cmp_name_read",
      description = "",
      groups = List(RoleMember("group1", isAdmin = false)),
      users = List(
        RoleMember("user1", isAdmin = false),
        RoleMember("user2", isAdmin = false),
        RoleMember("srvRole", isAdmin = true)),
      roles = Seq.empty
    )

    val refs: Set[CdpIamPrincipals] = Set(
      CdpIamUser("", "user1", ""),
      CdpIamUser("", "user2", ""),
      CdpIamGroup("group1", "")
    )

    val rangerClient = stub[RangerClient]

    val rangerRoleGateway = stub[RangerRoleGateway]
    val err = UpsertRoleErr(UpdateRoleErr(userRole, HttpErrors.ServerErr(500, "err")))
    (rangerRoleGateway.upsertRole _)
      .when(
        "domain_dp-name_0_cmp-name",
        UserRoleType,
        *,
        *,
        *,
        List("user1", "user2"),
        List("group1"))
      .returns(Left(err))

    val policyGateway = stub[RangerPolicyGateway]
    val securityZoneGateway = stub[RangerSecurityZoneGateway]
    val rangerGatewayProvider = stub[RangerGatewayProvider]
    (rangerGatewayProvider.getRangerGatewaysFromClient _)
      .when(*)
      .returns(Right(new RangerGateway(policyGateway, securityZoneGateway, rangerRoleGateway)))

    val expected = Left(ComponentGatewayError(show"$err"))

    val principalsMapper = stub[PrincipalsMapper[CdpIamPrincipals]]
    val impalaOutputPortAccessControlGateway = new ImpalaAccessControlGateway(
      serviceRole = "srvRole",
      rangerGatewayProvider = rangerGatewayProvider,
      principalsMapper = principalsMapper
    )

    val accessControlInfo = AccessControlInfo(
      dataProductOwner = "dataProductOwner",
      devGroup = "devGroup",
      componentId = "urn:dmb:cmp:domain:dp-name:0:cmp-name"
    )

    val actual =
      impalaOutputPortAccessControlGateway.updateAcl(accessControlInfo, refs, rangerClient)

    assert(actual == expected)
  }

  test("provision access control for simple table without user role") {

    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        StorageAreaFaker(ImpalaStorageAreaCdw(
          databaseName = "databaseName",
          tableName = "tableName",
          format = Csv,
          location = "loc",
          partitions = None,
          tableSchema = Seq(
            Column(
              "id",
              ColumnDataType.INT,
              None,
              None,
              None,
              None,
              None,
              None,
              None,
              None,
              None,
              None)
          )
        ).asJson).build()
      )
      .build()

    val datalake = new Datalake()
    datalake.setEnvironmentCrn("cdpEnvCrn")
    datalake.setDatalakeName("dlName")

    val externalTable = ExternalTable(
      "databaseName",
      "tableName",
      Seq(Field("id", ImpalaDataType.ImpalaInt, None)),
      Seq.empty,
      "loc",
      Csv)

    val hostProvider = stub[CDPPublicHostProvider]
    (hostProvider.getRangerHost _)
      .when(*)
      .returns(Right("rangerHost"))

    val policyGateway = stub[RangerPolicyGateway]
    (policyGateway.upsertPolicies _)
      .when(*, *, *, *, None, *, *)
      .returns(
        Right(
          Seq(
            PolicyAttachment("123", "xy"),
            PolicyAttachment("456", "ttt"),
            PolicyAttachment("789", "loc")
          ))
      )

    val rangerRoleGateway = stub[RangerRoleGateway]
    (rangerRoleGateway.upsertRole _)
      .when("domain_dp-name_0", OwnerRoleType, *, *, *, *, *)
      .returns(
        Right(
          RangerRole(
            id = 1,
            isEnabled = true,
            name = "domain_dp_name_0_owner",
            description = "",
            groups = Seq(RoleMember("devGroup", isAdmin = false)),
            users = Seq(
              RoleMember("srvRole", isAdmin = true),
              RoleMember("dataProductOwner", isAdmin = false)),
            roles = Seq.empty
          )
        )
      )

    val securityZoneGateway = stub[RangerSecurityZoneGateway]
    (securityZoneGateway.upsertSecurityZone _)
      .when(
        "srvRole",
        "domain_dp_name_0",
        "dataProductOwner",
        Some("devGroup"),
        "hive",
        "dlName",
        Seq("loc"),
        false)
      .returns(
        Right(
          RangerSecurityZone(
            1,
            "domain_dp_name_0",
            Map(
              "cm_hive" -> RangerSecurityZoneResources(
                Seq(
                  Map(
                    "database" -> Seq("domain_*"),
                    "column"   -> Seq("*"),
                    "table"    -> Seq("*")
                  ),
                  Map(
                    "url" -> Seq("loc/*")
                  )
                )
              )
            ),
            isEnabled = true,
            List("srvRole", "dataProductOwner"),
            List("devGroup"),
            List("srvRole", "dataProductOwner"),
            List("devGroup")
          )
        )
      )

    val rangerClient = stub[RangerClient]
    val rangerGatewayProvider = stub[RangerGatewayProvider]
    (rangerGatewayProvider.getRangerGatewaysFromClient _)
      .when(*)
      .returns(Right(new RangerGateway(policyGateway, securityZoneGateway, rangerRoleGateway)))

    val principalsMapper = stub[PrincipalsMapper[CdpIamPrincipals]]
    (principalsMapper.map _)
      .when(Set("dataProductOwner", "devGroup"))
      .returns(
        Map(
          "dataProductOwner" -> Right(CdpIamUser("", "dataProductOwner", "")),
          "devGroup"         -> Right(CdpIamGroup("devGroup", ""))
        ))

    val expected = Right(
      Seq(
        PolicyAttachment("123", "xy"),
        PolicyAttachment("456", "ttt"),
        PolicyAttachment("789", "loc")
      ))

    val impalaOutputPortAccessControlGateway = new ImpalaAccessControlGateway(
      serviceRole = "srvRole",
      rangerGatewayProvider = rangerGatewayProvider,
      principalsMapper = principalsMapper
    )

    val accessControlInfo = AccessControlInfo(
      dataProductOwner = "dataProductOwner",
      devGroup = "devGroup",
      componentId = "urn:dmb:cmp:domain:dp-name:0:cmp-name"
    )

    val actual =
      impalaOutputPortAccessControlGateway.provisionAccessControl(
        accessControlInfo,
        rangerClient,
        externalTable,
        "dlName",
        provisionUserRole = false)

    assert(actual == expected)
  }

  test("unprovision access control without user role") {

    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        OutputPortFaker[Json](
          PublicImpalaCdw(
            databaseName = "databaseName",
            tableName = "tableName",
            cdpEnvironment = "cdpEnv",
            cdwVirtualWarehouse = "service",
            format = Csv,
            location = "loc",
            partitions = None
          ).asJson).build()
      )
      .build()

    val externalTable = ExternalTable(
      "databaseName",
      "tableName",
      Seq(Field("id", ImpalaDataType.ImpalaInt, None)),
      Seq.empty,
      "loc",
      Csv)

    val hostProvider = stub[CDPPublicHostProvider]
    (hostProvider.getRangerHost _)
      .when(*)
      .returns(Right("rangerHost"))

    val policyGateway = stub[RangerPolicyGateway]
    (policyGateway.deletePolicies _)
      .when(*, *, *, None, *)
      .returns(
        Right(
          Seq(
            PolicyAttachment("456", "ttt"),
            PolicyAttachment("789", "loc")
          ))
      )

    val securityZoneGateway = stub[RangerSecurityZoneGateway]
    (securityZoneGateway.upsertSecurityZone _)
      .when(
        "srvRole",
        "domain_dp_name_0",
        "dataProductOwner",
        Some("devGroup"),
        "hive",
        "dlName",
        Seq("loc"),
        true)
      .returns(
        Right(
          RangerSecurityZone(
            1,
            "domain_dp_name_0",
            Map(
              "cm_hive" -> RangerSecurityZoneResources(
                Seq(
                  Map(
                    "database" -> Seq("domain_*"),
                    "column"   -> Seq("*"),
                    "table"    -> Seq("*")
                  ),
                  Map(
                    "url" -> Seq.empty[String]
                  )
                )
              )
            ),
            isEnabled = true,
            List("srvRole", "dataProductOwner"),
            List("devGroup"),
            List("srvRole", "dataProductOwner"),
            List("devGroup")
          )
        )
      )

    val rangerRoleGateway = stub[RangerRoleGateway]

    val rangerGatewayProvider = stub[RangerGatewayProvider]
    (rangerGatewayProvider.getRangerGatewaysFromClient _)
      .when(*)
      .returns(Right(new RangerGateway(policyGateway, securityZoneGateway, rangerRoleGateway)))

    val rangerClient = stub[RangerClient]

    val principalsMapper = stub[PrincipalsMapper[CdpIamPrincipals]]
    (principalsMapper.map _)
      .when(Set("dataProductOwner", "devGroup"))
      .returns(
        Map(
          "dataProductOwner" -> Right(CdpIamUser("", "dataProductOwner", "")),
          "devGroup"         -> Right(CdpIamGroup("devGroup", ""))
        ))

    val expected = Right(
      Seq(
        PolicyAttachment("456", "ttt"),
        PolicyAttachment("789", "loc")
      ))

    val impalaOutputPortAccessControlGateway = new ImpalaAccessControlGateway(
      serviceRole = "srvRole",
      rangerGatewayProvider = rangerGatewayProvider,
      principalsMapper = principalsMapper
    )

    val accessControlInfo = AccessControlInfo(
      dataProductOwner = "dataProductOwner",
      devGroup = "devGroup",
      componentId = "urn:dmb:cmp:domain:dp-name:0:cmp-name"
    )

    val actual =
      impalaOutputPortAccessControlGateway.unprovisionAccessControl(
        accessControlInfo,
        rangerClient,
        externalTable,
        "dlName",
        unprovisionUserRole = false)

    assert(actual == expected)
  }

}
