package it.agilelab.provisioning.impala.table.provisioner.gateway.resource

import cats.implicits.{ showInterpolator, toShow }
import com.cloudera.cdp.datalake.model.Datalake
import com.cloudera.cdp.environments.model.Environment
import io.circe.Json
import io.circe.generic.auto.exportEncoder
import io.circe.syntax.EncoderOps
import it.agilelab.provisioning.commons.client.ranger.RangerClient
import it.agilelab.provisioning.commons.client.ranger.model.{ RangerRole, RoleMember }
import it.agilelab.provisioning.commons.principalsmapping.{ CdpIamPrincipals, CdpIamUser }
import it.agilelab.provisioning.impala.table.provisioner.clients.cdp.CDPPublicHostProvider
import it.agilelab.provisioning.impala.table.provisioner.common.{
  OutputPortFaker,
  ProvisionRequestFaker
}
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaFormat.Csv
import it.agilelab.provisioning.impala.table.provisioner.core.model._
import it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.provider.RangerGatewayProvider
import it.agilelab.provisioning.impala.table.provisioner.gateway.resource.acl.{
  AccessControlInfo,
  ImpalaAccessControlGateway
}
import it.agilelab.provisioning.impala.table.provisioner.gateway.table.ExternalTableGateway
import it.agilelab.provisioning.mesh.self.service.api.model.Component.DataContract
import it.agilelab.provisioning.mesh.self.service.api.model.openmetadata.{ Column, ColumnDataType }
import it.agilelab.provisioning.mesh.self.service.core.model.ProvisionCommand
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

class ImpalaOutputPortGatewayTest extends AnyFunSuite with MockFactory {

  test("provision simple table") {

    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        OutputPortFaker(
          PublicImpalaTableCdw(
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

    val environment = new Environment()
    environment.setCrn("cdpEnvCrn")

    val datalake = new Datalake()
    datalake.setEnvironmentCrn("cdpEnvCrn")
    datalake.setDatalakeName("dlName")

    val hostProvider = stub[CDPPublicHostProvider]
    (hostProvider.getEnvironment _)
      .when(*)
      .returns(Right(environment))

    (hostProvider.getDataLake _)
      .when(*)
      .returns(Right(datalake))

    (hostProvider.getImpalaCoordinatorHost _)
      .when(*, *)
      .returns(Right("impalaHost"))

    (hostProvider.getRangerHost _)
      .when(datalake)
      .returns(Right("http://rangerHost/ranger/"))

    val externalTableGateway = stub[ExternalTableGateway]
    (externalTableGateway.create _)
      .when(*, *, *)
      .returns(Right())

    val impalaAccessControlGateway = stub[ImpalaAccessControlGateway]
    (impalaAccessControlGateway.provisionAccessControl _)
      .when(*, *, *, *, true)
      .returns(
        Right(
          Seq(
            PolicyAttachment("123", "xy"),
            PolicyAttachment("456", "ttt"),
            PolicyAttachment("789", "loc")
          ))
      )

    val rangerGatewayProvider = stub[RangerGatewayProvider]
    val rangerClient = stub[RangerClient]
    (rangerGatewayProvider.getRangerClient _)
      .when("http://rangerHost/ranger/")
      .returns(
        Right(rangerClient)
      )

    val impalaTableOutputPortGateway =
      new ImpalaTableOutputPortGateway(
        "srvRole",
        hostProvider,
        externalTableGateway,
        rangerGatewayProvider,
        impalaAccessControlGateway
      )

    val provisionCommand = ProvisionCommand("requestId", request)
    val actual = impalaTableOutputPortGateway.create(provisionCommand)

    val expected = Right(
      ImpalaEntityResource(
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
    actual match {
      case e: Right[_, _] => assert(e == expected)
      case Left(value)    => fail(value.error, value)
    }
  }

  test("provision partitioned table") {

    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        OutputPortFaker(PublicImpalaTableCdw(
          databaseName = "databaseName",
          tableName = "tableName",
          cdpEnvironment = "cdpEnv",
          cdwVirtualWarehouse = "service",
          format = Csv,
          location = "loc",
          partitions = Some(Seq("part1"))
        ).asJson)
          .withDataContract(DataContract(
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
          ))
          .build()
      )
      .build()

    val environment = new Environment()
    environment.setCrn("cdpEnvCrn")

    val datalake = new Datalake()
    datalake.setEnvironmentCrn("cdpEnvCrn")
    datalake.setDatalakeName("dlName")

    val hostProvider = stub[CDPPublicHostProvider]
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

    val impalaAccessControlGateway = stub[ImpalaAccessControlGateway]
    (impalaAccessControlGateway.provisionAccessControl _)
      .when(*, *, *, *, true)
      .returns(
        Right(
          Seq(
            PolicyAttachment("123", "xy"),
            PolicyAttachment("456", "ttt"),
            PolicyAttachment("789", "loc")
          ))
      )

    val rangerGatewayProvider = stub[RangerGatewayProvider]
    val rangerClient = stub[RangerClient]
    (rangerGatewayProvider.getRangerClient _)
      .when(*)
      .returns(
        Right(rangerClient)
      )

    val impalaTableOutputPortGateway =
      new ImpalaTableOutputPortGateway(
        "srvRole",
        hostProvider,
        externalTableGateway,
        rangerGatewayProvider,
        impalaAccessControlGateway
      )

    val provisionCommand = ProvisionCommand("requestId", request)

    val actual = impalaTableOutputPortGateway.create(provisionCommand)

    val expected = Right(
      ImpalaEntityResource(
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

    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        OutputPortFaker(
          PublicImpalaTableCdw(
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

    val environment = new Environment()
    environment.setCrn("cdpEnvCrn")

    val datalake = new Datalake()
    datalake.setEnvironmentCrn("cdpEnvCrn")
    datalake.setDatalakeName("dlName")

    val hostProvider = stub[CDPPublicHostProvider]
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
    (externalTableGateway.drop _)
      .when(*, *, *)
      .returns(Right())

    val impalaAccessControlGateway = stub[ImpalaAccessControlGateway]
    (impalaAccessControlGateway.unprovisionAccessControl _)
      .when(*, *, *, *, true)
      .returns(
        Right(
          Seq(
            PolicyAttachment("456", "ttt"),
            PolicyAttachment("789", "loc")
          ))
      )

    val rangerGatewayProvider = stub[RangerGatewayProvider]
    val rangerClient = stub[RangerClient]
    (rangerGatewayProvider.getRangerClient _)
      .when(*)
      .returns(
        Right(rangerClient)
      )

    val impalaTableOutputPortGateway =
      new ImpalaTableOutputPortGateway(
        "srvRole",
        hostProvider,
        externalTableGateway,
        rangerGatewayProvider,
        impalaAccessControlGateway
      )

    val provisionCommand = ProvisionCommand("requestId", request)
    val actual = impalaTableOutputPortGateway.destroy(provisionCommand)

    val expected = Right(
      ImpalaEntityResource(
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

  test("updateacl") {
    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        OutputPortFaker(
          PublicImpalaTableCdw(
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

    val refs: Set[CdpIamPrincipals] =
      Set(CdpIamUser("uid", "username", "crn"), CdpIamUser("uid", "username2", "crn"))

    val environment = new Environment()
    environment.setCrn("cdpEnvCrn")

    val datalake = new Datalake()
    datalake.setEnvironmentCrn("cdpEnvCrn")
    datalake.setDatalakeName("dlName")

    val hostProvider = stub[CDPPublicHostProvider]
    (hostProvider.getEnvironment _)
      .when(*)
      .returns(Right(environment))

    (hostProvider.getDataLake _)
      .when(*)
      .returns(Right(datalake))

    (hostProvider.getRangerHost _)
      .when(*)
      .returns(Right("rangerHost"))

    val rangerGatewayProvider = stub[RangerGatewayProvider]
    val rangerClient = stub[RangerClient]
    (rangerGatewayProvider.getRangerClient _)
      .when(*)
      .returns(
        Right(rangerClient)
      )

    val externalTableGateway = stub[ExternalTableGateway]

    val accessControlInfo = AccessControlInfo(
      dataProductOwner = "dataProductOwner",
      devGroup = "devGroup",
      componentId = "urn:dmb:cmp:domain:dp-name:0:cmp-name"
    )

    val impalaAccessControlGateway = stub[ImpalaAccessControlGateway]
    (impalaAccessControlGateway.updateAcl _)
      .when(accessControlInfo, refs, rangerClient)
      .returns(
        Right(
          RangerRole(
            id = 10,
            isEnabled = true,
            name = "domain_dp_name_0_databaseName_tableName_read",
            description = "description",
            groups = Seq.empty,
            users = Seq(
              RoleMember("username", isAdmin = false),
              RoleMember("username2", isAdmin = false)),
            roles = Seq.empty
          )
        )
      )

    val impalaTableOutputPortGateway =
      new ImpalaTableOutputPortGateway(
        "srvRole",
        hostProvider,
        externalTableGateway,
        rangerGatewayProvider,
        impalaAccessControlGateway
      )

    val provisionCommand = ProvisionCommand("requestId", request)
    val actual = impalaTableOutputPortGateway.updateAcl(provisionCommand, refs)

    val expected = Right(refs)

    assert(actual == expected)
  }
}
