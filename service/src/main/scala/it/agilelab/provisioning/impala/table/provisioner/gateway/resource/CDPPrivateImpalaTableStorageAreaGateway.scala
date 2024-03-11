package it.agilelab.provisioning.impala.table.provisioner.gateway.resource

import cats.implicits.{ showInterpolator, toBifunctorOps }
import io.circe.Json
import io.circe.generic.auto._
import it.agilelab.provisioning.commons.config.ConfError.ConfKeyNotFoundErr
import it.agilelab.provisioning.commons.principalsmapping.CdpIamPrincipals
import it.agilelab.provisioning.impala.table.provisioner.clients.cdp.ConfigHostProvider
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider.ConnectionConfig
import it.agilelab.provisioning.impala.table.provisioner.context.ApplicationConfiguration
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaCdw.ImpalaProvisionRequestOps
import it.agilelab.provisioning.impala.table.provisioner.core.model._
import it.agilelab.provisioning.impala.table.provisioner.gateway.mapper.{
  ExternalTableMapper,
  ImpalaViewMapper
}
import it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.provider.RangerGatewayProvider
import it.agilelab.provisioning.impala.table.provisioner.gateway.resource.acl.{
  AccessControlInfo,
  ImpalaAccessControlGateway
}
import it.agilelab.provisioning.impala.table.provisioner.gateway.table.ExternalTableGateway
import it.agilelab.provisioning.impala.table.provisioner.gateway.view.ViewGateway
import it.agilelab.provisioning.mesh.self.service.api.model.Component.{ OutputPort, StorageArea }
import it.agilelab.provisioning.mesh.self.service.api.model.openmetadata.Column
import it.agilelab.provisioning.mesh.self.service.core.gateway.{
  ComponentGateway,
  ComponentGatewayError,
  PermissionlessComponentGateway
}
import it.agilelab.provisioning.mesh.self.service.core.model.ProvisionCommand

import scala.util.Try

class CDPPrivateImpalaTableStorageAreaGateway(
    serviceRole: String,
    hostProvider: ConfigHostProvider,
    externalTableGateway: ExternalTableGateway,
    viewGateway: ViewGateway,
    rangerGatewayProvider: RangerGatewayProvider,
    impalaAccessControlGateway: ImpalaAccessControlGateway
) extends PermissionlessComponentGateway[Json, Json, ImpalaEntityResource] {

  /** Creates all the resources for the Storage Area.
    *
    * It creates the Impala external table based on the location defined in the descriptor;
    * creates or updates the necessary security zones;
    * database, table and location policies for the defined owners in the component descriptor,
    *
    * @param provisionCommand A Provision Command including the provisioning request containing the data product descriptor
    * @return Either a [[ComponentGatewayError]] if an error occurred while creating the storage area,
    *         or an [[ImpalaEntityResource]] that includes the information of the newly deployed Storage Area.
    */
  override def create(
      provisionCommand: ProvisionCommand[Json, Json]
  ): Either[ComponentGatewayError, ImpalaEntityResource] = for {
    // Extract necessary information
    storageAreaRequest <- provisionCommand.provisionRequest
      .getStorageAreaRequest[ImpalaCdw]
      .leftMap(e => ComponentGatewayError(show"$e"))
    // Upsert storage area
    impalaEntity <- {
      val entity: Either[ComponentGatewayError, ImpalaEntity] = storageAreaRequest.specific match {
        case table: PrivateImpalaStorageAreaCdw =>
          createAndGetExternalTable(table.tableSchema, table)
        case view: PrivateImpalaStorageAreaViewCdw =>
          createAndGetView(view)
        case other =>
          Left(
            ComponentGatewayError(
              "Received wrongly formatted specific schema. " +
                "The schema doesn't belong to a table or view storage area for CDP Private Cloud."))
      }
      entity
    }
    // Upsert security and access
    rangerHost <- hostProvider.getRangerHost
      .leftMap(e => ComponentGatewayError(show"$e"))
    rangerClient <- rangerGatewayProvider
      .getRangerClient(rangerHost)
      .leftMap(e => ComponentGatewayError(show"$e"))
    policies <- impalaAccessControlGateway.provisionAccessControl(
      AccessControlInfo(
        provisionCommand.provisionRequest.dataProduct.dataProductOwner,
        provisionCommand.provisionRequest.dataProduct.devGroup,
        storageAreaRequest.id),
      rangerClient,
      impalaEntity,
      "",
      provisionUserRole = false
    )
  } yield ImpalaEntityResource(impalaEntity, ImpalaCdpAcl(policies, List.empty[PolicyAttachment]))

  override def destroy(
      unprovisionCommand: ProvisionCommand[Json, Json]
  ): Either[ComponentGatewayError, ImpalaEntityResource] = for {
    // Extract necessary information
    storageAreaRequest <- unprovisionCommand.provisionRequest
      .getStorageAreaRequest[ImpalaCdw]
      .leftMap(e => ComponentGatewayError(show"$e"))
    dropOnUnprovision <- Try {
      ApplicationConfiguration.impalaConfig.getBoolean(ApplicationConfiguration.DROP_ON_UNPROVISION)
    }.toEither.leftMap { _ =>
      val confError = ConfKeyNotFoundErr(ApplicationConfiguration.DROP_ON_UNPROVISION)
      ComponentGatewayError(show"$confError")
    }
    // Upsert/delete output port
    impalaEntity <- {
      val entity: Either[ComponentGatewayError, ImpalaEntity] = storageAreaRequest.specific match {
        case table: PrivateImpalaStorageAreaCdw =>
          if (dropOnUnprovision) {
            dropAndGetExternalTable(table.tableSchema, table)
          } else {
            ExternalTableMapper.map(table.tableSchema, table)
          }
        case view: PrivateImpalaStorageAreaViewCdw =>
          if (dropOnUnprovision) {
            dropAndGetView(view)
          } else {
            // SA Views don't receive a schema, but a query, so if not provided, schema is empty
            ImpalaViewMapper.map(view.tableSchema.getOrElse(List.empty), view)
          }
        case other =>
          Left(
            ComponentGatewayError(
              "Received wrongly formatted specific schema. " +
                "The schema doesn't belong to a table or view storage area for CDP Private Cloud."))
      }
      entity
    }

    // Update security and access
    rangerHost <- hostProvider.getRangerHost
      .leftMap(e => ComponentGatewayError(show"$e"))
    rangerClient <- rangerGatewayProvider
      .getRangerClient(rangerHost)
      .leftMap(e => ComponentGatewayError(show"$e"))
    policies <- impalaAccessControlGateway.unprovisionAccessControl(
      AccessControlInfo(
        unprovisionCommand.provisionRequest.dataProduct.dataProductOwner,
        unprovisionCommand.provisionRequest.dataProduct.devGroup,
        storageAreaRequest.id),
      rangerClient,
      impalaEntity,
      "",
      unprovisionUserRole = false
    )
  } yield ImpalaEntityResource(impalaEntity, ImpalaCdpAcl(List.empty[PolicyAttachment], policies))

  private def createExternalTable(
      connectionConfig: ConnectionConfig,
      externalTable: ExternalTable
  ): Either[ComponentGatewayError, Unit] =
    externalTableGateway
      .create(connectionConfig, externalTable, ifNotExists = true)
      .leftMap(e => ComponentGatewayError(show"$e"))

  private def dropExternalTable(
      connectionConfig: ConnectionConfig,
      externalTable: ExternalTable
  ): Either[ComponentGatewayError, Unit] =
    externalTableGateway
      .drop(connectionConfig, externalTable, ifExists = true)
      .leftMap(e => ComponentGatewayError(show"$e"))

  private def createAndGetExternalTable(
      schema: Seq[Column],
      impalaSpecific: ImpalaTableCdw
  ): Either[ComponentGatewayError, ExternalTable] = for {
    impalaHost <- hostProvider
      .getImpalaCoordinatorHost()
      .leftMap(e => ComponentGatewayError(show"$e"))
    externalTable <- ExternalTableMapper.map(schema, impalaSpecific)
    connectionConfig <- ConnectionConfig
      .getFromConfig(
        ApplicationConfiguration.impalaConfig.getConfig(ApplicationConfiguration.JDBC_CONFIG),
        impalaHost)
      .leftMap(e => ComponentGatewayError(show"$e"))
    _ <- createExternalTable(connectionConfig, externalTable)
  } yield externalTable

  private def dropAndGetExternalTable(
      schema: Seq[Column],
      impalaSpecific: ImpalaTableCdw
  ): Either[ComponentGatewayError, ExternalTable] = for {
    impalaHost <- hostProvider
      .getImpalaCoordinatorHost()
      .leftMap(e => ComponentGatewayError(show"$e"))
    externalTable <- ExternalTableMapper.map(schema, impalaSpecific)
    connectionConfig <- ConnectionConfig
      .getFromConfig(
        ApplicationConfiguration.impalaConfig.getConfig(ApplicationConfiguration.JDBC_CONFIG),
        impalaHost)
      .leftMap(e => ComponentGatewayError(show"$e"))
    _ <- dropExternalTable(connectionConfig, externalTable)
  } yield externalTable

  // View Storage Area

  private def createView(
      connectionConfig: ConnectionConfig,
      impalaView: ImpalaView
  ): Either[ComponentGatewayError, Unit] =
    viewGateway
      .create(
        connectionConfig,
        impalaView,
        ifNotExists = true
      )
      .leftMap(e => ComponentGatewayError(show"$e"))

  private def dropView(
      connectionConfig: ConnectionConfig,
      impalaView: ImpalaView
  ): Either[ComponentGatewayError, Unit] =
    viewGateway
      .drop(
        connectionConfig,
        impalaView,
        ifExists = true
      )
      .leftMap(e => ComponentGatewayError(show"$e"))

  private def createAndGetView(
      impalaSpecific: PrivateImpalaStorageAreaViewCdw
  ): Either[ComponentGatewayError, ImpalaView] = for {
    impalaHost <- hostProvider
      .getImpalaCoordinatorHost()
      .leftMap(e => ComponentGatewayError(show"$e"))
    // SA Views don't receive a schema, but a query, so if not provided, schema is empty
    impalaView <- ImpalaViewMapper.map(
      impalaSpecific.tableSchema.getOrElse(List.empty),
      impalaSpecific)
    connectionConfig <- ConnectionConfig
      .getFromConfig(
        ApplicationConfiguration.impalaConfig.getConfig(ApplicationConfiguration.JDBC_CONFIG),
        impalaHost)
      .leftMap(e => ComponentGatewayError(show"$e"))
    _ <- createView(connectionConfig, impalaView)
  } yield impalaView

  private def dropAndGetView(
      impalaSpecific: PrivateImpalaStorageAreaViewCdw
  ): Either[ComponentGatewayError, ImpalaView] = for {
    impalaHost <- hostProvider
      .getImpalaCoordinatorHost()
      .leftMap(e => ComponentGatewayError(show"$e"))
    // SA Views don't receive a schema, but a query, so if not provided, schema is empty
    impalaView <- ImpalaViewMapper.map(
      impalaSpecific.tableSchema.getOrElse(List.empty),
      impalaSpecific)
    connectionConfig <- ConnectionConfig
      .getFromConfig(
        ApplicationConfiguration.impalaConfig.getConfig(ApplicationConfiguration.JDBC_CONFIG),
        impalaHost)
      .leftMap(e => ComponentGatewayError(show"$e"))
    _ <- dropView(connectionConfig, impalaView)
  } yield impalaView

}
