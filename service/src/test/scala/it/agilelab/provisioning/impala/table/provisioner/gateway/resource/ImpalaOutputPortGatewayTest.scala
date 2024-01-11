package it.agilelab.provisioning.impala.table.provisioner.gateway.resource

import com.cloudera.cdp.datalake.model.Datalake
import com.cloudera.cdp.environments.model.Environment
import io.circe.Json
import it.agilelab.provisioning.impala.table.provisioner.clients.cdp.HostProvider
import it.agilelab.provisioning.impala.table.provisioner.common.{
  OutputPortFaker,
  ProvisionRequestFaker
}
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaFormat.Csv
import it.agilelab.provisioning.impala.table.provisioner.core.model._
import it.agilelab.provisioning.impala.table.provisioner.gateway.table.ExternalTableGateway
import it.agilelab.provisioning.mesh.self.service.api.model.Component.DataContract
import it.agilelab.provisioning.mesh.self.service.api.model.openmetadata.{ Column, ColumnDataType }
import it.agilelab.provisioning.mesh.self.service.core.model.ProvisionCommand
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

class ImpalaOutputPortGatewayTest extends AnyFunSuite with MockFactory {

  test("provision simple table") {

    val request = ProvisionRequestFaker[Json, ImpalaCdw](Json.obj())
      .withComponent(
        OutputPortFaker(
          ImpalaCdw(
            databaseName = "databaseName",
            tableName = "tableName",
            cdpEnvironment = "cdpEnv",
            cdwVirtualWarehouse = "service",
            format = Csv,
            location = "loc",
            partitions = None
          )).build()
      )
      .build()

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

    (hostProvider.getImpalaCoordinatorHost _)
      .when(*, *)
      .returns(Right("impalaHost"))

    val externalTableGateway = stub[ExternalTableGateway]
    (externalTableGateway.create _)
      .when(*, *, *)
      .returns(Right())

    val impalaAccessControlGateway = stub[ImpalaOutputPortAccessControlGateway]
    (impalaAccessControlGateway.provisionAccessControl _)
      .when(*, *, *)
      .returns(
        Right(
          Seq(
            PolicyAttachment("123", "xy"),
            PolicyAttachment("456", "ttt"),
            PolicyAttachment("789", "loc")
          ))
      )

    val impalaTableOutputPortGateway =
      new ImpalaTableOutputPortGateway(
        "srvRole",
        hostProvider,
        externalTableGateway,
        impalaAccessControlGateway
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

    val request = ProvisionRequestFaker[Json, ImpalaCdw](Json.obj())
      .withComponent(
        OutputPortFaker(ImpalaCdw(
          databaseName = "databaseName",
          tableName = "tableName",
          cdpEnvironment = "cdpEnv",
          cdwVirtualWarehouse = "service",
          format = Csv,
          location = "loc",
          partitions = Some(Seq("part1"))
        ))
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

    val impalaAccessControlGateway = stub[ImpalaOutputPortAccessControlGateway]
    (impalaAccessControlGateway.provisionAccessControl _)
      .when(*, *, *)
      .returns(
        Right(
          Seq(
            PolicyAttachment("123", "xy"),
            PolicyAttachment("456", "ttt"),
            PolicyAttachment("789", "loc")
          ))
      )

    val impalaTableOutputPortGateway =
      new ImpalaTableOutputPortGateway(
        "srvRole",
        hostProvider,
        externalTableGateway,
        impalaAccessControlGateway
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

    val request = ProvisionRequestFaker[Json, ImpalaCdw](Json.obj())
      .withComponent(
        OutputPortFaker(
          ImpalaCdw(
            databaseName = "databaseName",
            tableName = "tableName",
            cdpEnvironment = "cdpEnv",
            cdwVirtualWarehouse = "service",
            format = Csv,
            location = "loc",
            partitions = None
          )).build()
      )
      .build()

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
    (externalTableGateway.drop _)
      .when(*, *, *)
      .returns(Right())

    val impalaAccessControlGateway = stub[ImpalaOutputPortAccessControlGateway]
    (impalaAccessControlGateway.unprovisionAccessControl _)
      .when(*, *, *)
      .returns(
        Right(
          Seq(
            PolicyAttachment("456", "ttt"),
            PolicyAttachment("789", "loc")
          ))
      )

    val impalaTableOutputPortGateway =
      new ImpalaTableOutputPortGateway(
        "srvRole",
        hostProvider,
        externalTableGateway,
        impalaAccessControlGateway
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

  test("updateacl") {}
}
