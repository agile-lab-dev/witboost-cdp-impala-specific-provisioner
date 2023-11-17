package it.agilelab.provisioning.impala.table.provisioner.gateway.resource

import com.cloudera.cdp.datalake.model.Datalake
import com.cloudera.cdp.environments.model.Environment
import it.agilelab.provisioning.commons.client.ranger.RangerClient
import it.agilelab.provisioning.commons.client.ranger.model.{
  RangerSecurityZone,
  RangerSecurityZoneResources
}
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaFormat.Csv
import it.agilelab.provisioning.mesh.repository.Repository
import it.agilelab.provisioning.mesh.self.service.api.model.Component.{ DataContract, OutputPort }
import it.agilelab.provisioning.mesh.self.service.api.model.openmetadata.{ Column, ColumnDataType }
import it.agilelab.provisioning.mesh.self.service.api.model.{ DataProduct, ProvisionRequest }
import it.agilelab.provisioning.mesh.self.service.core.model.ProvisionCommand
import it.agilelab.provisioning.mesh.self.service.lambda.core.model.Domain
import io.circe.Json
import it.agilelab.provisioning.impala.table.provisioner.clients.cdp.HostProvider
import it.agilelab.provisioning.impala.table.provisioner.core.model.{
  Acl,
  ExternalTable,
  Field,
  ImpalaCdpAcl,
  ImpalaCdw,
  ImpalaDataType,
  ImpalaTableOutputPortResource,
  PolicyAttachment
}
import it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.policy.RangerPolicyGateway
import it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.provider.RangerGatewayProvider
import it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.zone.RangerSecurityZoneGateway
import it.agilelab.provisioning.impala.table.provisioner.gateway.table.ExternalTableGateway
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

class ImpalaOutputPortGatewayTest extends AnyFunSuite with MockFactory {

  test("provision simple table") {
    val environment = new Environment()
    environment.setCrn("cdpEnvCrn")

    val datalake = new Datalake()
    datalake.setEnvironmentCrn("cdpEnvCrn")
    datalake.setDatalakeName("dlName")

    val hostProvider = stub[HostProvider]
    (hostProvider.getEnvironment _)
      .when(*)
      .returns(Right(environment))

    (hostProvider.getDataLake _)
      .when(*)
      .returns(Right(datalake))

    (hostProvider.getRangerHost _)
      .when(*)
      .returns(Right("rangerHost"))

    (hostProvider.getImpalaCoordinatorHost _)
      .when(*, *)
      .returns(Right("impalaHost"))

    val externalTableGateway = stub[ExternalTableGateway]
    (externalTableGateway.create _)
      .when(*, *, *)
      .returns(Right())

    val policyGateway = stub[RangerPolicyGateway]
    (policyGateway.attachPolicy _)
      .when(*, *, *, *, *, *, *)
      .returns(
        Right(
          Seq(
            PolicyAttachment("123", "xy"),
            PolicyAttachment("456", "ttt"),
            PolicyAttachment("789", "loc")
          ))
      )

    val securityZoneGateway = stub[RangerSecurityZoneGateway]
    (securityZoneGateway.upsertSecurityZone _)
      .when("srvRole", Domain("domain:name", "domain:name"), "hive", "dlName", Seq("loc"), false)
      .returns(
        Right(
          RangerSecurityZone(
            1,
            "plt",
            Map(
              "cm_hive" -> RangerSecurityZoneResources(
                Seq(
                  Map(
                    "database" -> Seq("plt_*"),
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
            List("adminUser1", "adminUser2"),
            List("adminUserGroup1", "adminUserGroup2"),
            List("auditUser1", "auditUser2"),
            List("auditUserGroup1", "auditUserGroup2")
          )
        )
      )

    val rangerClient = mock[RangerClient]
    val rangerGatewayProvider = stub[RangerGatewayProvider]
    (rangerGatewayProvider.getRangerClient _).when(*).returns(Right(rangerClient))
    (rangerGatewayProvider.getRangerPolicyGateway _).when(*).returns(Right(policyGateway))
    (rangerGatewayProvider.getRangerSecurityZoneGateway _)
      .when(*)
      .returns(Right(securityZoneGateway))

    val impalaTableOutputPortGateway =
      new ImpalaTableOutputPortGateway(
        "srvRole",
        hostProvider,
        //domainRepository,
        externalTableGateway,
        rangerGatewayProvider
      )

    val request: ProvisionRequest[Json, ImpalaCdw] =
      ProvisionRequest(
        dataProduct = DataProduct[Json](
          id = "urn:dmb:dp:plt:dp-name:0",
          name = "dp-name",
          domain = "domain:name",
          environment = "poc",
          version = "0.0.1",
          dataProductOwner = "dataProductOwner",
          specific = Json.obj(),
          components = Seq.empty
        ),
        component = Some(
          OutputPort[ImpalaCdw](
            id = "urn:dmb:cmp:plt:dp-name:0:cmp-name",
            name = "cmp-name",
            description = "description",
            version = "0.0.1",
            dataContract = DataContract(
              schema = Seq(
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
            ),
            specific = ImpalaCdw(
              databaseName = "databaseName",
              tableName = "tableName",
              cdpEnvironment = "cdpEnv",
              cdwVirtualWarehouse = "service",
              format = Csv,
              location = "loc",
              acl = Acl(
                owners = Seq("own"),
                users = Seq("us")
              ),
              partitions = None
            )
          ))
      )

    val provisionCommand = ProvisionCommand("requestId", request)
    val actual = impalaTableOutputPortGateway.create(provisionCommand)

    val expected = Right(
      ImpalaTableOutputPortResource(
        ExternalTable(
          "databaseName",
          "tableName",
          Seq(Field("id", ImpalaDataType.ImpalaInt, None)),
          Seq.empty,
          "loc",
          Csv),
        ImpalaCdpAcl(
          Seq(
            PolicyAttachment("123", "xy"),
            PolicyAttachment("456", "ttt"),
            PolicyAttachment("789", "loc")
          ),
          Seq.empty[PolicyAttachment]
        )
      )
    )
    assert(actual == expected)
  }

  test("provision partitioned table") {
    val environment = new Environment()
    environment.setCrn("cdpEnvCrn")

    val datalake = new Datalake()
    datalake.setEnvironmentCrn("cdpEnvCrn")
    datalake.setDatalakeName("dlName")

    val hostProvider = stub[HostProvider]
    (hostProvider.getEnvironment _)
      .when(*)
      .returns(Right(environment))

    (hostProvider.getDataLake _)
      .when(*)
      .returns(Right(datalake))

    (hostProvider.getRangerHost _)
      .when(*)
      .returns(Right("rangerHost"))

    (hostProvider.getImpalaCoordinatorHost _)
      .when(*, *)
      .returns(Right("impalaHost"))

    val externalTableGateway = stub[ExternalTableGateway]
    (externalTableGateway.create _)
      .when(*, *, *)
      .returns(Right())

    val policyGateway = stub[RangerPolicyGateway]
    (policyGateway.attachPolicy _)
      .when(*, *, *, *, *, *, *)
      .returns(
        Right(
          Seq(
            PolicyAttachment("123", "xy"),
            PolicyAttachment("456", "ttt"),
            PolicyAttachment("789", "loc")
          ))
      )

    val securityZoneGateway = stub[RangerSecurityZoneGateway]
    (securityZoneGateway.upsertSecurityZone _)
      .when("srvRole", Domain("domain:name", "domain:name"), "hive", "dlName", Seq("loc"), false)
      .returns(
        Right(
          RangerSecurityZone(
            1,
            "plt",
            Map(
              "cm_hive" -> RangerSecurityZoneResources(
                Seq(
                  Map(
                    "database" -> Seq("plt_*"),
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
            List("adminUser1", "adminUser2"),
            List("adminUserGroup1", "adminUserGroup2"),
            List("auditUser1", "auditUser2"),
            List("auditUserGroup1", "auditUserGroup2")
          )
        )
      )

    val rangerClient = mock[RangerClient]
    val rangerGatewayProvider = stub[RangerGatewayProvider]
    (rangerGatewayProvider.getRangerClient _).when(*).returns(Right(rangerClient))
    (rangerGatewayProvider.getRangerPolicyGateway _).when(*).returns(Right(policyGateway))
    (rangerGatewayProvider.getRangerSecurityZoneGateway _)
      .when(*)
      .returns(Right(securityZoneGateway))

    val impalaTableOutputPortGateway =
      new ImpalaTableOutputPortGateway(
        "srvRole",
        hostProvider,
        externalTableGateway,
        rangerGatewayProvider
      )

    val request: ProvisionRequest[Json, ImpalaCdw] =
      ProvisionRequest(
        dataProduct = DataProduct[Json](
          id = "urn:dmb:dp:plt:dp-name:0",
          name = "dp-name",
          domain = "domain:name",
          environment = "poc",
          version = "0.0.1",
          dataProductOwner = "dataProductOwner",
          specific = Json.obj(),
          components = Seq.empty
        ),
        component = Some(
          OutputPort[ImpalaCdw](
            id = "urn:dmb:cmp:plt:dp-name:0:cmp-name",
            name = "cmp-name",
            description = "description",
            version = "0.0.1",
            dataContract = DataContract(
              schema = Seq(
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
                  None),
                Column(
                  "part1",
                  ColumnDataType.STRING,
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
            ),
            specific = ImpalaCdw(
              databaseName = "databaseName",
              tableName = "tableName",
              cdpEnvironment = "cdpEnv",
              cdwVirtualWarehouse = "service",
              format = Csv,
              location = "loc",
              acl = Acl(
                owners = Seq("own"),
                users = Seq("us")
              ),
              partitions = Some(Seq("part1"))
            )
          ))
      )
    val provisionCommand = ProvisionCommand("requestId", request)

    val actual = impalaTableOutputPortGateway.create(provisionCommand)

    val expected = Right(
      ImpalaTableOutputPortResource(
        ExternalTable(
          "databaseName",
          "tableName",
          Seq(Field("id", ImpalaDataType.ImpalaInt, None)),
          Seq(Field("part1", ImpalaDataType.ImpalaString, None)),
          "loc",
          Csv
        ),
        ImpalaCdpAcl(
          Seq(
            PolicyAttachment("123", "xy"),
            PolicyAttachment("456", "ttt"),
            PolicyAttachment("789", "loc")
          ),
          Seq.empty[PolicyAttachment]
        )
      )
    )
    assert(actual == expected)
  }

  test("destroy table") {
    val environment = new Environment()
    environment.setCrn("cdpEnvCrn")

    val datalake = new Datalake()
    datalake.setEnvironmentCrn("cdpEnvCrn")
    datalake.setDatalakeName("dlName")

    val hostProvider = stub[HostProvider]
    (hostProvider.getEnvironment _)
      .when(*)
      .returns(Right(environment))

    (hostProvider.getDataLake _)
      .when(*)
      .returns(Right(datalake))

    (hostProvider.getRangerHost _)
      .when(*)
      .returns(Right("rangerHost"))

    (hostProvider.getImpalaCoordinatorHost _)
      .when(*, *)
      .returns(Right("impalaHost"))

    val externalTableGateway = stub[ExternalTableGateway]
    (externalTableGateway.create _)
      .when(*, *, *)
      .returns(Right())

    val policyGateway = stub[RangerPolicyGateway]
    (policyGateway.detachPolicy _)
      .when(*, *, *, *, *, *, *)
      .returns(
        Right(
          Seq(
            PolicyAttachment("456", "ttt"),
            PolicyAttachment("789", "loc")
          ))
      )

    val securityZoneGateway = stub[RangerSecurityZoneGateway]
    (securityZoneGateway.upsertSecurityZone _)
      .when("srvRole", Domain("domain:name", "domain:name"), "hive", "dlName", Seq("loc"), true)
      .returns(
        Right(
          RangerSecurityZone(
            1,
            "plt",
            Map(
              "cm_hive" -> RangerSecurityZoneResources(
                Seq(
                  Map(
                    "database" -> Seq("plt_*"),
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
            List("adminUser1", "adminUser2"),
            List("adminUserGroup1", "adminUserGroup2"),
            List("auditUser1", "auditUser2"),
            List("auditUserGroup1", "auditUserGroup2")
          )
        )
      )

    val rangerClient = mock[RangerClient]
    val rangerGatewayProvider = stub[RangerGatewayProvider]
    (rangerGatewayProvider.getRangerClient _).when(*).returns(Right(rangerClient))
    (rangerGatewayProvider.getRangerPolicyGateway _).when(*).returns(Right(policyGateway))
    (rangerGatewayProvider.getRangerSecurityZoneGateway _)
      .when(*)
      .returns(Right(securityZoneGateway))

    val impalaTableOutputPortGateway =
      new ImpalaTableOutputPortGateway(
        "srvRole",
        hostProvider,
        //domainRepository,
        externalTableGateway,
        rangerGatewayProvider
      )

    val request: ProvisionRequest[Json, ImpalaCdw] =
      ProvisionRequest(
        dataProduct = DataProduct[Json](
          id = "urn:dmb:dp:plt:dp-name:0",
          name = "dp-name",
          domain = "domain:name",
          environment = "poc",
          version = "0.0.1",
          dataProductOwner = "dataProductOwner",
          specific = Json.obj(),
          components = Seq.empty
        ),
        component = Some(
          OutputPort[ImpalaCdw](
            id = "urn:dmb:cmp:plt:dp-name:0:cmp-name",
            name = "cmp-name",
            description = "description",
            version = "0.0.1",
            dataContract = DataContract(
              schema = Seq(
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
            ),
            specific = ImpalaCdw(
              databaseName = "databaseName",
              tableName = "tableName",
              cdpEnvironment = "cdpEnv",
              cdwVirtualWarehouse = "service",
              format = Csv,
              location = "loc",
              acl = Acl(
                owners = Seq("own"),
                users = Seq("us")
              ),
              partitions = None
            )
          ))
      )

    val provisionCommand = ProvisionCommand("requestId", request)
    val actual = impalaTableOutputPortGateway.destroy(provisionCommand)

    val expected = Right(
      ImpalaTableOutputPortResource(
        ExternalTable(
          "databaseName",
          "tableName",
          Seq(Field("id", ImpalaDataType.ImpalaInt, None)),
          Seq.empty,
          "loc",
          Csv),
        ImpalaCdpAcl(
          Seq.empty[PolicyAttachment],
          Seq(
            PolicyAttachment("456", "ttt"),
            PolicyAttachment("789", "loc")
          )
        )
      )
    )
    assert(actual == expected)
  }

}
