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
import it.agilelab.provisioning.mesh.self.service.api.model.Component.OutputPort
import it.agilelab.provisioning.mesh.self.service.api.model.openmetadata.Column
import it.agilelab.provisioning.mesh.self.service.core.gateway.{
  ComponentGateway,
  ComponentGatewayError
}
import it.agilelab.provisioning.mesh.self.service.core.model.ProvisionCommand

import scala.util.Try

class CDPPrivateImpalaOutputPortGateway(
    serviceRole: String,
    hostProvider: ConfigHostProvider,
    externalTableGateway: ExternalTableGateway,
    viewGateway: ViewGateway,
    rangerGatewayProvider: RangerGatewayProvider,
    impalaAccessControlGateway: ImpalaAccessControlGateway
) extends ComponentGateway[Json, Json, ImpalaProvisionerResource, CdpIamPrincipals] {

  /** Creates all the resources for the Output Port.
    *
    * It supports the creation of either Impala external table or Impala views based on the location defined in the descriptor;
    * creates or updates the necessary security zones;
    * database, table and location policies for the defined owners and users in the component descriptor,
    *
    * @param provisionCommand A Provision Command including the provisioning request containing the data product descriptor
    * @return Either a [[ComponentGatewayError]] if an error occurred while creating the output port,
    *         or an [[ImpalaProvisionerResource]] that includes the information of the newly deployed Output Port.
    */
  override def create(
      provisionCommand: ProvisionCommand[Json, Json]
  ): Either[ComponentGatewayError, ImpalaProvisionerResource] =
    for {
      // Extract necessary information
      opRequest <- provisionCommand.provisionRequest
        .getOutputPortRequest[ImpalaCdw]
        .leftMap(e => ComponentGatewayError(show"$e"))
      // Upsert output port
      createdResource <- opRequest.specific match {
        case table: PrivateImpalaTableCdw =>
          createAndGetExternalTable(opRequest.dataContract.schema, table)
        case view: PrivateImpalaViewCdw =>
          createAndGetView(opRequest.dataContract.schema, view)
        case other =>
          Left(
            ComponentGatewayError(
              "Received wrongly formatted specific schema. " +
                "The schema doesn't belong to an output port table or view for CDP Private Cloud."))
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
          opRequest.id),
        rangerClient,
        createdResource.impalaEntity,
        "",
        provisionUserRole = true
      )
    } yield ImpalaProvisionerResource(
      createdResource,
      ImpalaCdpAcl(policies, Seq.empty[PolicyAttachment]))

  override def destroy(
      unprovisionCommand: ProvisionCommand[Json, Json]
  ): Either[ComponentGatewayError, ImpalaProvisionerResource] = for {
    // Extract necessary information
    opRequest <- unprovisionCommand.provisionRequest
      .getOutputPortRequest[ImpalaCdw]
      .leftMap(e => ComponentGatewayError(show"$e"))
    dropOnUnprovision <- Try {
      ApplicationConfiguration.impalaConfig.getBoolean(ApplicationConfiguration.DROP_ON_UNPROVISION)
    }.toEither.leftMap { _ =>
      val confError = ConfKeyNotFoundErr(ApplicationConfiguration.DROP_ON_UNPROVISION)
      ComponentGatewayError(show"$confError")
    }
    // Upsert/delete output port
    entityResource <-
      opRequest.specific match {
        case table: PrivateImpalaTableCdw =>
          if (dropOnUnprovision) {
            dropAndGetExternalTable(opRequest.dataContract.schema, table)
          } else {
            ExternalTableMapper
              .map(opRequest.dataContract.schema, table)
              .map(ImpalaEntityResource(_, ""))
          }
        case view: PrivateImpalaViewCdw =>
          if (dropOnUnprovision) {
            dropAndGetView(opRequest.dataContract.schema, view)
          } else {
            ImpalaViewMapper
              .map(opRequest.dataContract.schema, view)
              .map(ImpalaEntityResource(_, ""))
          }
        case _ =>
          Left(
            ComponentGatewayError(
              "Received wrongly formatted specific schema. " +
                "The schema doesn't belong to an output port table or view for CDP Private Cloud."))
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
        opRequest.id),
      rangerClient,
      entityResource.impalaEntity,
      "",
      unprovisionUserRole = true
    )
  } yield ImpalaProvisionerResource(
    entityResource,
    ImpalaCdpAcl(Seq.empty[PolicyAttachment], policies))

  override def updateAcl(
      provisionCommand: ProvisionCommand[Json, Json],
      refs: Set[CdpIamPrincipals]
  ): Either[ComponentGatewayError, Set[CdpIamPrincipals]] = for {
    opRequest <- provisionCommand.provisionRequest
      .getOutputPortRequest[ImpalaCdw]
      .leftMap(e => ComponentGatewayError(show"$e"))
      .flatMap(op =>
        op.specific match {
          case _: PrivateImpalaTableCdw | _: PrivateImpalaViewCdw => Right(op)
          case _ =>
            Left(
              ComponentGatewayError("Received wrongly formatted specific schema. " +
                "The schema doesn't belong to an output port table or view for CDP Private Cloud."))
        })
    rangerHost <- hostProvider.getRangerHost
      .leftMap(e => ComponentGatewayError(show"$e"))
    rangerClient <- rangerGatewayProvider
      .getRangerClient(rangerHost)
      .leftMap(e => ComponentGatewayError(show"$e"))
    _ <- impalaAccessControlGateway.updateAcl(
      AccessControlInfo(
        provisionCommand.provisionRequest.dataProduct.dataProductOwner,
        provisionCommand.provisionRequest.dataProduct.devGroup,
        opRequest.id),
      refs,
      rangerClient
    )
  } yield refs

  // External Table Output Port

  private def createExternalTable(
      connectionConfig: ConnectionConfig,
      externalTable: ExternalTable
  ): Either[ComponentGatewayError, ImpalaEntityResource] =
    externalTableGateway
      .create(connectionConfig, externalTable, ifNotExists = true)
      .leftMap(e => ComponentGatewayError(show"$e"))

  private def dropExternalTable(
      connectionConfig: ConnectionConfig,
      externalTable: ExternalTable
  ): Either[ComponentGatewayError, ImpalaEntityResource] =
    externalTableGateway
      .drop(connectionConfig, externalTable, ifExists = true)
      .leftMap(e => ComponentGatewayError(show"$e"))

  private def createAndGetExternalTable(
      schema: Seq[Column],
      impalaSpecific: ImpalaTableCdw
  ): Either[ComponentGatewayError, ImpalaEntityResource] = for {
    impalaHost <- hostProvider
      .getImpalaCoordinatorHost()
      .leftMap(e => ComponentGatewayError(show"$e"))
    externalTable <- ExternalTableMapper.map(schema, impalaSpecific)
    connectionConfig <- ConnectionConfig
      .getFromConfig(
        ApplicationConfiguration.impalaConfig.getConfig(ApplicationConfiguration.JDBC_CONFIG),
        impalaHost)
      .leftMap(e => ComponentGatewayError(show"$e"))
    createdResource <- createExternalTable(connectionConfig, externalTable)
  } yield createdResource

  private def dropAndGetExternalTable(
      schema: Seq[Column],
      impalaSpecific: ImpalaTableCdw
  ): Either[ComponentGatewayError, ImpalaEntityResource] = for {
    impalaHost <- hostProvider
      .getImpalaCoordinatorHost()
      .leftMap(e => ComponentGatewayError(show"$e"))
    externalTable <- ExternalTableMapper.map(schema, impalaSpecific)
    connectionConfig <- ConnectionConfig
      .getFromConfig(
        ApplicationConfiguration.impalaConfig.getConfig(ApplicationConfiguration.JDBC_CONFIG),
        impalaHost)
      .leftMap(e => ComponentGatewayError(show"$e"))
    droppedResource <- dropExternalTable(connectionConfig, externalTable)
  } yield droppedResource

  // View Output Port

  private def createView(
      connectionConfig: ConnectionConfig,
      impalaView: ImpalaView
  ): Either[ComponentGatewayError, ImpalaEntityResource] =
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
  ): Either[ComponentGatewayError, ImpalaEntityResource] =
    viewGateway
      .drop(
        connectionConfig,
        impalaView,
        ifExists = true
      )
      .leftMap(e => ComponentGatewayError(show"$e"))

  private def createAndGetView(
      schema: Seq[Column],
      impalaSpecific: PrivateImpalaViewCdw
  ): Either[ComponentGatewayError, ImpalaEntityResource] = for {
    impalaHost <- hostProvider
      .getImpalaCoordinatorHost()
      .leftMap(e => ComponentGatewayError(show"$e"))
    impalaView <- ImpalaViewMapper.map(schema, impalaSpecific)
    connectionConfig <- ConnectionConfig
      .getFromConfig(
        ApplicationConfiguration.impalaConfig.getConfig(ApplicationConfiguration.JDBC_CONFIG),
        impalaHost)
      .leftMap(e => ComponentGatewayError(show"$e"))
    createdResource <- createView(connectionConfig, impalaView)
  } yield createdResource

  private def dropAndGetView(
      schema: Seq[Column],
      impalaSpecific: PrivateImpalaViewCdw
  ): Either[ComponentGatewayError, ImpalaEntityResource] = for {
    impalaHost <- hostProvider
      .getImpalaCoordinatorHost()
      .leftMap(e => ComponentGatewayError(show"$e"))
    impalaView <- ImpalaViewMapper.map(schema, impalaSpecific)
    connectionConfig <- ConnectionConfig
      .getFromConfig(
        ApplicationConfiguration.impalaConfig.getConfig(ApplicationConfiguration.JDBC_CONFIG),
        impalaHost)
      .leftMap(e => ComponentGatewayError(show"$e"))
    droppedResource <- dropView(connectionConfig, impalaView)
  } yield droppedResource

}
