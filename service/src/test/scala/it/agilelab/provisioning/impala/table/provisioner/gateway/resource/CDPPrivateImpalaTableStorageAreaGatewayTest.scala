package it.agilelab.provisioning.impala.table.provisioner.gateway.resource

import io.circe.Json
import io.circe.generic.auto.exportEncoder
import io.circe.syntax.EncoderOps
import it.agilelab.provisioning.commons.client.ranger.RangerClient
import it.agilelab.provisioning.commons.principalsmapping.{ CdpIamPrincipals, CdpIamUser }
import it.agilelab.provisioning.impala.table.provisioner.clients.cdp.ConfigHostProvider
import it.agilelab.provisioning.impala.table.provisioner.common.{
  OutputPortFaker,
  ProvisionRequestFaker,
  StorageAreaFaker
}
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaFormat.Csv
import it.agilelab.provisioning.impala.table.provisioner.core.model._
import it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.provider.RangerGatewayProvider
import it.agilelab.provisioning.impala.table.provisioner.gateway.resource.acl.ImpalaAccessControlGateway
import it.agilelab.provisioning.impala.table.provisioner.gateway.table.ExternalTableGateway
import it.agilelab.provisioning.impala.table.provisioner.gateway.view.ViewGateway
import it.agilelab.provisioning.mesh.self.service.api.model.openmetadata.{ Column, ColumnDataType }
import it.agilelab.provisioning.mesh.self.service.core.gateway.ComponentGatewayError
import it.agilelab.provisioning.mesh.self.service.core.model.ProvisionCommand
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

class CDPPrivateImpalaTableStorageAreaGatewayTest extends AnyFunSuite with MockFactory {

  test("provision simple storage area table") {
    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        StorageAreaFaker(PrivateImpalaStorageAreaCdw(
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

    val hostProvider = stub[ConfigHostProvider]
    (hostProvider.getImpalaCoordinatorHost _)
      .when(*)
      .returns(Right("impalaHost"))

    (hostProvider.getRangerHost _)
      .when()
      .returns(Right("http://rangerHost/ranger/"))

    val externalTableGateway = stub[ExternalTableGateway]
    (externalTableGateway.create _)
      .when(*, *, *)
      .returns(Right())

    val impalaAccessControlGateway = stub[ImpalaAccessControlGateway]
    (impalaAccessControlGateway.provisionAccessControl _)
      .when(*, *, *, *, false)
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

    val impalaTableStorageAreaGateway =
      new CDPPrivateImpalaTableStorageAreaGateway(
        "srvRole",
        hostProvider,
        externalTableGateway,
        viewGateway,
        rangerGatewayProvider,
        impalaAccessControlGateway
      )

    val provisionCommand = ProvisionCommand("requestId", request)
    val actual = impalaTableStorageAreaGateway.create(provisionCommand)

    val expected =
      ImpalaEntityResource(
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
        ImpalaCdpAcl(
          Seq(
            PolicyAttachment("123", "xy"),
            PolicyAttachment("456", "ttt"),
            PolicyAttachment("789", "loc")
          ),
          Seq.empty[PolicyAttachment]
        )
      )

    actual match {
      case Right(value) => assert(value == expected)
      case Left(value)  => fail(value.error, value)
    }
  }

  test("provision partitioned table") {

    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        StorageAreaFaker(PrivateImpalaStorageAreaCdw(
          databaseName = "databaseName",
          tableName = "tableName",
          format = Csv,
          location = "loc",
          partitions = Some(Seq("part1")),
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
          ),
          tableParams = None
        ).asJson).build()
      )
      .build()

    val hostProvider = stub[ConfigHostProvider]
    (hostProvider.getRangerHost _)
      .when()
      .returns(Right("rangerHost"))

    (hostProvider.getImpalaCoordinatorHost _)
      .when(*)
      .returns(Right("impalaHost"))

    val externalTableGateway = stub[ExternalTableGateway]
    (externalTableGateway.create _)
      .when(*, *, *)
      .returns(Right())

    val impalaAccessControlGateway = stub[ImpalaAccessControlGateway]
    (impalaAccessControlGateway.provisionAccessControl _)
      .when(*, *, *, *, false)
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

    val viewGateway = stub[ViewGateway]

    val impalaTableStorageAreaGateway =
      new CDPPrivateImpalaTableStorageAreaGateway(
        "srvRole",
        hostProvider,
        externalTableGateway,
        viewGateway,
        rangerGatewayProvider,
        impalaAccessControlGateway
      )

    val provisionCommand = ProvisionCommand("requestId", request)

    val actual = impalaTableStorageAreaGateway.create(provisionCommand)

    val expected = Right(
      ImpalaEntityResource(
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
        StorageAreaFaker(PrivateImpalaStorageAreaCdw(
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
      .returns(Right())

    val impalaAccessControlGateway = stub[ImpalaAccessControlGateway]
    (impalaAccessControlGateway.unprovisionAccessControl _)
      .when(*, *, *, *, false)
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

    val impalaTableStorageAreaGateway =
      new CDPPrivateImpalaTableStorageAreaGateway(
        "srvRole",
        hostProvider,
        externalTableGateway,
        viewGateway,
        rangerGatewayProvider,
        impalaAccessControlGateway
      )

    val provisionCommand = ProvisionCommand("requestId", request)
    val actual = impalaTableStorageAreaGateway.destroy(provisionCommand)

    val expected = Right(
      ImpalaEntityResource(
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

  // View Storage Area

  test("provision storage area view") {
    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        StorageAreaFaker(
          PrivateImpalaStorageAreaViewCdw(
            databaseName = "databaseName",
            viewName = "viewName",
            queryStatement = "SELECT * FROM otherDb.otherTableName",
            tableSchema = None).asJson)
          .build()
      )
      .build()

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
      .when(*, *, *, *, false)
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
      .when(*, *, *)
      .returns(Right())

    val impalaTableStorageAreaGateway =
      new CDPPrivateImpalaTableStorageAreaGateway(
        "srvRole",
        hostProvider,
        externalTableGateway,
        viewGateway,
        rangerGatewayProvider,
        impalaAccessControlGateway
      )

    val provisionCommand = ProvisionCommand("requestId", request)
    val actual = impalaTableStorageAreaGateway.create(provisionCommand)

    val expected =
      ImpalaEntityResource(
        ImpalaView(
          "databaseName",
          "viewName",
          Seq.empty,
          None,
          Some("SELECT * FROM otherDb.otherTableName")),
        ImpalaCdpAcl(
          Seq(
            PolicyAttachment("123", "xy"),
            PolicyAttachment("456", "ttt"),
            PolicyAttachment("789", "loc")
          ),
          Seq.empty[PolicyAttachment]
        )
      )

    actual match {
      case Right(value) => assert(value == expected)
      case Left(value)  => fail(value.error, value)
    }
  }

  test("drop storage area view") {
    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        StorageAreaFaker(
          PrivateImpalaStorageAreaViewCdw(
            databaseName = "databaseName",
            viewName = "viewName",
            queryStatement = "SELECT * FROM otherDb.otherTableName",
            tableSchema = None).asJson)
          .build()
      )
      .build()

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
      .when(*, *, *, *, false)
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
      .when(*, *, *)
      .returns(Right())

    val impalaTableStorageAreaGateway =
      new CDPPrivateImpalaTableStorageAreaGateway(
        "srvRole",
        hostProvider,
        externalTableGateway,
        viewGateway,
        rangerGatewayProvider,
        impalaAccessControlGateway
      )

    val provisionCommand = ProvisionCommand("requestId", request)
    val actual = impalaTableStorageAreaGateway.destroy(provisionCommand)

    val expected =
      ImpalaEntityResource(
        ImpalaView(
          "databaseName",
          "viewName",
          Seq.empty,
          None,
          Some("SELECT * FROM otherDb.otherTableName")),
        ImpalaCdpAcl(
          Seq.empty[PolicyAttachment],
          Seq(
            PolicyAttachment("456", "ttt"),
            PolicyAttachment("789", "loc")
          )
        )
      )

    actual match {
      case Right(value) => assert(value == expected)
      case Left(value)  => fail(value.error, value)
    }
  }

  // Wrong component

  test("provision on unsupported specific for storage area results in error") {
    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        StorageAreaFaker(
          PrivateImpalaViewCdw(
            databaseName = "databaseName",
            viewName = "viewName",
            tableName = "tableNameNonSupported").asJson)
          .build()
      )
      .build()

    val hostProvider = stub[ConfigHostProvider]
    val externalTableGateway = stub[ExternalTableGateway]
    val impalaAccessControlGateway = stub[ImpalaAccessControlGateway]
    val rangerGatewayProvider = stub[RangerGatewayProvider]
    val viewGateway = stub[ViewGateway]

    val impalaTableStorageAreaGateway =
      new CDPPrivateImpalaTableStorageAreaGateway(
        "srvRole",
        hostProvider,
        externalTableGateway,
        viewGateway,
        rangerGatewayProvider,
        impalaAccessControlGateway
      )

    val provisionCommand = ProvisionCommand("requestId", request)
    val actual = impalaTableStorageAreaGateway.create(provisionCommand)

    val expected =
      ComponentGatewayError(
        "Received wrongly formatted specific schema. " +
          "The schema doesn't belong to a table or view storage area for CDP Private Cloud.")

    actual match {
      case Right(_)    => fail(s"Answer was $actual expected Left()")
      case Left(value) => assert(value == expected)
    }
  }

  test("drop on unsupported specific for storage area results in error") {
    val request = ProvisionRequestFaker[Json, Json](Json.obj())
      .withComponent(
        StorageAreaFaker(
          PrivateImpalaViewCdw(
            databaseName = "databaseName",
            viewName = "viewName",
            tableName = "tableNameNonSupported").asJson)
          .build()
      )
      .build()

    val hostProvider = stub[ConfigHostProvider]
    val externalTableGateway = stub[ExternalTableGateway]
    val impalaAccessControlGateway = stub[ImpalaAccessControlGateway]
    val rangerGatewayProvider = stub[RangerGatewayProvider]
    val viewGateway = stub[ViewGateway]

    val impalaTableStorageAreaGateway =
      new CDPPrivateImpalaTableStorageAreaGateway(
        "srvRole",
        hostProvider,
        externalTableGateway,
        viewGateway,
        rangerGatewayProvider,
        impalaAccessControlGateway
      )

    val provisionCommand = ProvisionCommand("requestId", request)
    val actual = impalaTableStorageAreaGateway.destroy(provisionCommand)

    val expected =
      ComponentGatewayError(
        "Received wrongly formatted specific schema. " +
          "The schema doesn't belong to a table or view storage area for CDP Private Cloud.")

    actual match {
      case Right(_)    => fail(s"Answer was $actual expected Left()")
      case Left(value) => assert(value == expected)
    }
  }

  test("updateacl") {
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
    val rangerGatewayProvider = stub[RangerGatewayProvider]
    val rangerClient = stub[RangerClient]
    val externalTableGateway = stub[ExternalTableGateway]
    val impalaAccessControlGateway = stub[ImpalaAccessControlGateway]
    val viewGateway = stub[ViewGateway]

    val impalaTableStorageAreaGateway =
      new CDPPrivateImpalaTableStorageAreaGateway(
        "srvRole",
        hostProvider,
        externalTableGateway,
        viewGateway,
        rangerGatewayProvider,
        impalaAccessControlGateway
      )

    val provisionCommand = ProvisionCommand("requestId", request)
    val actual = impalaTableStorageAreaGateway.updateAcl(provisionCommand, refs)

    val expected = Left(ComponentGatewayError("Update ACL is not a supported operation"))

    assert(actual == expected)

  }

}
