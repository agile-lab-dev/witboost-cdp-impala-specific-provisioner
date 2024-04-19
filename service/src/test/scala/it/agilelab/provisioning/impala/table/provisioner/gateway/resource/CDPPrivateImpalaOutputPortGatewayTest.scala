package it.agilelab.provisioning.impala.table.provisioner.gateway.resource

import io.circe.Json
import io.circe.generic.auto.exportEncoder
import io.circe.syntax.EncoderOps
import it.agilelab.provisioning.commons.client.ranger.RangerClient
import it.agilelab.provisioning.commons.client.ranger.model.{ RangerRole, RoleMember }
import it.agilelab.provisioning.commons.principalsmapping.{ CdpIamPrincipals, CdpIamUser }
import it.agilelab.provisioning.impala.table.provisioner.clients.cdp.ConfigHostProvider
import it.agilelab.provisioning.impala.table.provisioner.common.{
  OutputPortFaker,
  ProvisionRequestFaker
}
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaDataType.ImpalaInt
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaFormat.Csv
import it.agilelab.provisioning.impala.table.provisioner.core.model._
import it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.provider.RangerGatewayProvider
import it.agilelab.provisioning.impala.table.provisioner.gateway.resource.acl.{
  AccessControlInfo,
  ImpalaAccessControlGateway
}
import it.agilelab.provisioning.impala.table.provisioner.gateway.table.ExternalTableGateway
import it.agilelab.provisioning.impala.table.provisioner.gateway.view.ViewGateway
import it.agilelab.provisioning.mesh.self.service.api.model.Component.DataContract
import it.agilelab.provisioning.mesh.self.service.api.model.openmetadata.{ Column, ColumnDataType }
import it.agilelab.provisioning.mesh.self.service.core.gateway.ComponentGatewayError
import it.agilelab.provisioning.mesh.self.service.core.model.ProvisionCommand
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

class CDPPrivateImpalaOutputPortGatewayTest extends AnyFunSuite with MockFactory {

  test("provision simple table output port") {

    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        OutputPortFaker(
          PrivateImpalaTableCdw(
            databaseName = "databaseName",
            tableName = "tableName",
            format = Csv,
            location = "loc",
            partitions = None,
            tableParams = None
          ).asJson).build()
      )
      .build()

    val entityResource = ImpalaEntityResource(
      ExternalTable(
        "databaseName",
        "tableName",
        Seq(Field("id", ImpalaDataType.ImpalaInt, None)),
        Seq.empty,
        "loc",
        Csv,
        None,
        Map.empty,
        header = false),
      "jdbc://")

    val hostProvider = stub[ConfigHostProvider]
    (hostProvider.getImpalaCoordinatorHost _)
      .when(*)
      .returns(Right("impalaHost"))

    (hostProvider.getRangerHost _)
      .when()
      .returns(Right("http://rangerHost/ranger/"))

    val externalTableGateway = mock[ExternalTableGateway]
    val impalaAccessControlGateway = mock[ImpalaAccessControlGateway]

    // Provision output port, then access control and only at the end refresh
    inSequence {
      (externalTableGateway.create _)
        .expects(*, *, *)
        .returns(Right(entityResource))

      (impalaAccessControlGateway.provisionAccessControl _)
        .expects(*, *, *, *, true)
        .returns(
          Right(
            Seq(
              PolicyAttachment("123", "xy"),
              PolicyAttachment("456", "ttt"),
              PolicyAttachment("789", "loc")
            ))
        )

      (externalTableGateway.refresh _)
        .expects(*, *)
        .returns(Right(entityResource))
    }

    val rangerGatewayProvider = stub[RangerGatewayProvider]
    val rangerClient = stub[RangerClient]
    (rangerGatewayProvider.getRangerClient _)
      .when("http://rangerHost/ranger/")
      .returns(
        Right(rangerClient)
      )

    val viewGateway = stub[ViewGateway]

    val impalaTableOutputPortGateway =
      new CDPPrivateImpalaOutputPortGateway(
        "srvRole",
        hostProvider,
        externalTableGateway,
        viewGateway,
        rangerGatewayProvider,
        impalaAccessControlGateway
      )

    val provisionCommand = ProvisionCommand("requestId", request)
    val actual = impalaTableOutputPortGateway.create(provisionCommand)

    val expected = Right(
      ImpalaProvisionerResource(
        entityResource,
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

  test("provision partitioned table output port") {

    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        OutputPortFaker(
          PrivateImpalaTableCdw(
            databaseName = "databaseName",
            tableName = "tableName",
            format = Csv,
            location = "loc",
            partitions = Some(Seq("part1")),
            tableParams = None
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

    val entityResource = ImpalaEntityResource(
      ExternalTable(
        "databaseName",
        "tableName",
        Seq(Field("id", ImpalaDataType.ImpalaInt, None)),
        Seq(Field("part1", ImpalaDataType.ImpalaString, None)),
        "loc",
        Csv,
        None,
        Map.empty,
        header = false
      ),
      "jdbc://"
    )

    val hostProvider = stub[ConfigHostProvider]
    (hostProvider.getRangerHost _)
      .when()
      .returns(Right("rangerHost"))

    (hostProvider.getImpalaCoordinatorHost _)
      .when(*)
      .returns(Right("impalaHost"))

    val externalTableGateway = mock[ExternalTableGateway]
    val impalaAccessControlGateway = mock[ImpalaAccessControlGateway]

    // Provision output port, then access control and only at the end refresh
    inSequence {
      (externalTableGateway.create _)
        .expects(*, *, *)
        .returns(Right(entityResource))

      (impalaAccessControlGateway.provisionAccessControl _)
        .expects(*, *, *, *, true)
        .returns(
          Right(
            Seq(
              PolicyAttachment("123", "xy"),
              PolicyAttachment("456", "ttt"),
              PolicyAttachment("789", "loc")
            ))
        )

      (externalTableGateway.refresh _)
        .expects(*, *)
        .returns(Right(entityResource))
    }

    val rangerGatewayProvider = stub[RangerGatewayProvider]
    val rangerClient = stub[RangerClient]
    (rangerGatewayProvider.getRangerClient _)
      .when(*)
      .returns(
        Right(rangerClient)
      )

    val viewGateway = stub[ViewGateway]

    val impalaTableOutputPortGateway =
      new CDPPrivateImpalaOutputPortGateway(
        "srvRole",
        hostProvider,
        externalTableGateway,
        viewGateway,
        rangerGatewayProvider,
        impalaAccessControlGateway
      )

    val provisionCommand = ProvisionCommand("requestId", request)

    val actual = impalaTableOutputPortGateway.create(provisionCommand)

    val expected = Right(
      ImpalaProvisionerResource(
        entityResource,
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

  test("destroy table output port") {

    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        OutputPortFaker(
          PrivateImpalaTableCdw(
            databaseName = "databaseName",
            tableName = "tableName",
            format = Csv,
            location = "loc",
            partitions = None,
            tableParams = None
          ).asJson).build()
      )
      .build()

    val entityResource = ImpalaEntityResource(
      ExternalTable(
        "databaseName",
        "tableName",
        Seq(Field("id", ImpalaDataType.ImpalaInt, None)),
        Seq.empty,
        "loc",
        Csv,
        None,
        Map.empty,
        header = false),
      "jdbc://")

    val hostProvider = stub[ConfigHostProvider]

    (hostProvider.getRangerHost _)
      .when()
      .returns(Right("rangerHost"))

    (hostProvider.getImpalaCoordinatorHost _)
      .when(*)
      .returns(Right("impalaHost"))

    val externalTableGateway = stub[ExternalTableGateway]
    (externalTableGateway.drop _)
      .when(*, *, *)
      .returns(Right(entityResource))

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

    val viewGateway = stub[ViewGateway]

    val impalaTableOutputPortGateway =
      new CDPPrivateImpalaOutputPortGateway(
        "srvRole",
        hostProvider,
        externalTableGateway,
        viewGateway,
        rangerGatewayProvider,
        impalaAccessControlGateway
      )

    val provisionCommand = ProvisionCommand("requestId", request)
    val actual = impalaTableOutputPortGateway.destroy(provisionCommand)

    val expected = Right(
      ImpalaProvisionerResource(
        entityResource,
        ImpalaCdpAcl(
          Seq.empty[PolicyAttachment],
          Seq(
            PolicyAttachment("456", "ttt"),
            PolicyAttachment("789", "loc")
          )
        ))
    )
    assert(actual == expected)
  }

  test("updateacl on correct op specific schema") {
    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        OutputPortFaker(
          PrivateImpalaTableCdw(
            databaseName = "databaseName",
            tableName = "tableName",
            format = Csv,
            location = "loc",
            partitions = None,
            tableParams = None
          ).asJson).build()
      )
      .build()

    val refs: Set[CdpIamPrincipals] =
      Set(CdpIamUser("uid", "username", "crn"), CdpIamUser("uid", "username2", "crn"))

    val hostProvider = stub[ConfigHostProvider]

    (hostProvider.getRangerHost _)
      .when()
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

    val viewGateway = stub[ViewGateway]

    val impalaTableOutputPortGateway =
      new CDPPrivateImpalaOutputPortGateway(
        "srvRole",
        hostProvider,
        externalTableGateway,
        viewGateway,
        rangerGatewayProvider,
        impalaAccessControlGateway
      )

    val provisionCommand = ProvisionCommand("requestId", request)
    val actual = impalaTableOutputPortGateway.updateAcl(provisionCommand, refs)

    val expected = Right(refs)

    assert(actual == expected)
  }

  test("provision simple view") {

    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        OutputPortFaker(
          PrivateImpalaViewCdw(
            databaseName = "databaseName",
            source = ImpalaEntityIdentifierCdw("originalDatabase", "originalTableName"),
            viewName = "viewName"
          ).asJson).build()
      )
      .build()

    val view = ImpalaView(
      database = "databaseName",
      name = "viewName",
      schema = Seq(Field("id", ImpalaInt, None)),
      readsFromSource = Some(ImpalaEntityImpl("originalDatabase", "originalTableName", Seq.empty)),
      querySourceStatement = None
    )

    val entityResource = ImpalaEntityResource(view, "jdbc://")

    val hostProvider = stub[ConfigHostProvider]
    (hostProvider.getImpalaCoordinatorHost _)
      .when(*)
      .returns(Right("impalaHost"))

    (hostProvider.getRangerHost _)
      .when()
      .returns(Right("http://rangerHost/ranger/"))

    val externalTableGateway = stub[ExternalTableGateway]

    val impalaAccessControlGateway = stub[ImpalaAccessControlGateway]
    (impalaAccessControlGateway.provisionAccessControl _)
      .when(*, *, view, *, true)
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

    val viewGateway = stub[ViewGateway]
    (viewGateway.create _)
      .when(*, view, *)
      .returns(Right(entityResource))

    val impalaTableOutputPortGateway =
      new CDPPrivateImpalaOutputPortGateway(
        "srvRole",
        hostProvider,
        externalTableGateway,
        viewGateway,
        rangerGatewayProvider,
        impalaAccessControlGateway
      )

    val provisionCommand = ProvisionCommand("requestId", request)
    val actual = impalaTableOutputPortGateway.create(provisionCommand)

    val expected = Right(
      ImpalaProvisionerResource(
        entityResource,
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

  test("destroy simple view") {

    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        OutputPortFaker(
          PrivateImpalaViewCdw(
            databaseName = "databaseName",
            source = ImpalaEntityIdentifierCdw("originalDatabase", "originalTableName"),
            viewName = "viewName"
          ).asJson).build()
      )
      .build()

    val view = ImpalaView(
      database = "databaseName",
      name = "viewName",
      schema = Seq(Field("id", ImpalaInt, None)),
      readsFromSource = Some(ImpalaEntityImpl("originalDatabase", "originalTableName", Seq.empty)),
      querySourceStatement = None
    )

    val entityResource = ImpalaEntityResource(view, "jdbc://")

    val hostProvider = stub[ConfigHostProvider]

    (hostProvider.getRangerHost _)
      .when()
      .returns(Right("rangerHost"))

    (hostProvider.getImpalaCoordinatorHost _)
      .when(*)
      .returns(Right("impalaHost"))

    val externalTableGateway = stub[ExternalTableGateway]

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

    val viewGateway = stub[ViewGateway]
    (viewGateway.drop _)
      .when(*, view, *)
      .returns(Right(entityResource))

    val impalaTableOutputPortGateway =
      new CDPPrivateImpalaOutputPortGateway(
        "srvRole",
        hostProvider,
        externalTableGateway,
        viewGateway,
        rangerGatewayProvider,
        impalaAccessControlGateway
      )

    val provisionCommand = ProvisionCommand("requestId", request)
    val actual = impalaTableOutputPortGateway.destroy(provisionCommand)

    val expected = Right(
      ImpalaProvisionerResource(
        entityResource,
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

  test("update acl fails on wrong specific schema") {
    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        OutputPortFaker(PrivateImpalaStorageAreaCdw(
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
          ),
          tableParams = None
        ).asJson).build()
      )
      .build()

    val refs: Set[CdpIamPrincipals] =
      Set(CdpIamUser("uid", "username", "crn"), CdpIamUser("uid", "username2", "crn"))

    val hostProvider = stub[ConfigHostProvider]
    val rangerGatewayProvider = stub[RangerGatewayProvider]
    val rangerClient = stub[RangerClient]
    val externalTableGateway = stub[ExternalTableGateway]
    val impalaAccessControlGateway = stub[ImpalaAccessControlGateway]
    val viewGateway = stub[ViewGateway]

    val impalaTableOutputPortGateway =
      new CDPPrivateImpalaOutputPortGateway(
        "srvRole",
        hostProvider,
        externalTableGateway,
        viewGateway,
        rangerGatewayProvider,
        impalaAccessControlGateway
      )

    val provisionCommand = ProvisionCommand("requestId", request)
    val actual = impalaTableOutputPortGateway.updateAcl(provisionCommand, refs)

    val expected = Left(
      ComponentGatewayError("Received wrongly formatted specific schema. " +
        "The schema doesn't belong to an output port table or view for CDP Private Cloud."))

    assert(actual == expected)
  }

}
