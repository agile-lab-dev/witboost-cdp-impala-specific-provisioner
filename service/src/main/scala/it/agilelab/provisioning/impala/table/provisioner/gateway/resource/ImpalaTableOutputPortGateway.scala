package it.agilelab.provisioning.impala.table.provisioner.gateway.resource

import cats.implicits._
import com.cloudera.cdp.environments.model.Environment
import io.circe.Json
import io.circe.generic.auto._
import it.agilelab.provisioning.commons.config.ConfError.ConfKeyNotFoundErr
import it.agilelab.provisioning.commons.principalsmapping.CdpIamPrincipals
import it.agilelab.provisioning.impala.table.provisioner.clients.cdp.CDPPublicHostProvider
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider.ConnectionConfig
import it.agilelab.provisioning.impala.table.provisioner.context.ApplicationConfiguration
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaCdw.ImpalaProvisionRequestOps
import it.agilelab.provisioning.impala.table.provisioner.core.model._
import it.agilelab.provisioning.impala.table.provisioner.gateway.mapper.ExternalTableMapper
import it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.provider.RangerGatewayProvider
import it.agilelab.provisioning.impala.table.provisioner.gateway.resource.acl.{
  AccessControlInfo,
  ImpalaAccessControlGateway
}
import it.agilelab.provisioning.impala.table.provisioner.gateway.table.ExternalTableGateway
import it.agilelab.provisioning.mesh.self.service.api.model.Component.OutputPort
import it.agilelab.provisioning.mesh.self.service.core.gateway.{
  ComponentGateway,
  ComponentGatewayError
}
import it.agilelab.provisioning.mesh.self.service.core.model.ProvisionCommand

import scala.util.Try

class ImpalaTableOutputPortGateway(
    serviceRole: String,
    hostProvider: CDPPublicHostProvider,
    externalTableGateway: ExternalTableGateway,
    rangerGatewayProvider: RangerGatewayProvider,
    impalaAccessControlGateway: ImpalaAccessControlGateway
) extends ComponentGateway[Json, Json, ImpalaProvisionerResource, CdpIamPrincipals] {

  /** Creates all the resources for the Output Port.
    *
    * It creates the Impala external table based on the location defined in the descriptor;
    * creates or updates the necessary security zones;
    * database, table and location policies for the defined owners and users in the component descriptor,
    *
    * @param provisionCommand A Provision Command including the provisioning request containing the data product descriptor
    * @return Either a [[ComponentGatewayError]] if an error occurred while creating the output port,
    *         or an [[ImpalaProvisionerResource]] that includes the information of the newly deployed Output Port.
    */
  override def create(
      provisionCommand: ProvisionCommand[Json, Json]
  ): Either[ComponentGatewayError, ImpalaProvisionerResource] = for {
    // Extract necessary information
    opRequest <- provisionCommand.provisionRequest
      .getOutputPortRequest[PublicImpalaTableCdw]
      .leftMap(e => ComponentGatewayError(show"$e"))
    env <- hostProvider
      .getEnvironment(opRequest.specific.cdpEnvironment)
      .leftMap(e => ComponentGatewayError(show"$e"))
    dl <- hostProvider.getDataLake(env).leftMap(e => ComponentGatewayError(show"$e"))
    // Upsert output port
    createdResource <- createAndGetExternalTable(env, opRequest)
    // Upsert security and access
    rangerHost <- hostProvider
      .getRangerHost(dl)
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
      dl.getDatalakeName,
      provisionUserRole = true
    )
    // Refresh output port
    _ <- refreshAndGetExternalTable(env, opRequest)
  } yield ImpalaProvisionerResource(
    createdResource,
    ImpalaCdpAcl(policies, Seq.empty[PolicyAttachment]))

  /** Destroys all the resources for the Output Port.
    *
    * It updates the security zones to remove the data location as part of the managed resources;
    * deletes the table and location policies for the defined owners and users in the component descriptor,
    *
    * @param unprovisionCommand A Provision Command including the unprovisioning request containing the data product descriptor
    * @return Either a [[ComponentGatewayError]] if an error occurred while destroying the output port,
    *         or an [[ImpalaProvisionerResource]] that includes the information of the deleted Output Port.
    */
  override def destroy(
      unprovisionCommand: ProvisionCommand[Json, Json]
  ): Either[ComponentGatewayError, ImpalaProvisionerResource] = for {
    opRequest <- unprovisionCommand.provisionRequest
      .getOutputPortRequest[PublicImpalaTableCdw]
      .leftMap(e => ComponentGatewayError(show"$e"))
    env <- hostProvider
      .getEnvironment(opRequest.specific.cdpEnvironment)
      .leftMap(e => ComponentGatewayError(show"$e"))
    dl <- hostProvider.getDataLake(env).leftMap(e => ComponentGatewayError(show"$e"))
    dropOnUnprovision <- Try {
      ApplicationConfiguration.impalaConfig.getBoolean(ApplicationConfiguration.DROP_ON_UNPROVISION)
    }.toEither.leftMap { _ =>
      val confError = ConfKeyNotFoundErr(ApplicationConfiguration.DROP_ON_UNPROVISION)
      ComponentGatewayError(show"$confError")
    }
    droppedResource <-
      if (dropOnUnprovision) {
        dropAndGetExternalTable(env, opRequest)
      } else {
        ExternalTableMapper
          .map(opRequest.dataContract.schema, opRequest.specific)
          .map(ImpalaEntityResource(_, ""))
      }
    rangerHost <- hostProvider
      .getRangerHost(dl)
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
      droppedResource.impalaEntity,
      dl.getDatalakeName,
      unprovisionUserRole = true
    )
  } yield ImpalaProvisionerResource(
    droppedResource,
    ImpalaCdpAcl(Seq.empty[PolicyAttachment], policies))

  override def updateAcl(
      provisionCommand: ProvisionCommand[Json, Json],
      refs: Set[CdpIamPrincipals]
  ): Either[ComponentGatewayError, Set[CdpIamPrincipals]] = for {
    opRequest <- provisionCommand.provisionRequest
      .getOutputPortRequest[PublicImpalaTableCdw]
      .leftMap(e => ComponentGatewayError(show"$e"))
    env <- hostProvider
      .getEnvironment(opRequest.specific.cdpEnvironment)
      .leftMap(e => ComponentGatewayError(show"$e"))
    dl <- hostProvider.getDataLake(env).leftMap(e => ComponentGatewayError(show"$e"))
    rangerHost <- hostProvider
      .getRangerHost(dl)
      .leftMap(e => ComponentGatewayError(show"$e"))
    rangerClient <- rangerGatewayProvider
      .getRangerClient(rangerHost)
      .leftMap(e => ComponentGatewayError(show"$e"))
    role <- impalaAccessControlGateway.updateAcl(
      AccessControlInfo(
        provisionCommand.provisionRequest.dataProduct.dataProductOwner,
        provisionCommand.provisionRequest.dataProduct.devGroup,
        opRequest.id),
      refs,
      rangerClient
    )
  } yield refs

  private def createExternalTable(
      connectionConfig: ConnectionConfig,
      externalTable: ExternalTable
  ): Either[ComponentGatewayError, ImpalaEntityResource] =
    externalTableGateway
      .create(
        connectionConfig,
        externalTable,
        ifNotExists = true
      )
      .leftMap(e => ComponentGatewayError(show"$e"))

  private def refreshExternalTable(
      connectionConfig: ConnectionConfig,
      externalTable: ExternalTable
  ): Either[ComponentGatewayError, ImpalaEntityResource] =
    externalTableGateway
      .refresh(
        connectionConfig,
        externalTable
      )
      .leftMap(e => ComponentGatewayError(show"$e"))

  private def dropExternalTable(
      connectionConfig: ConnectionConfig,
      externalTable: ExternalTable
  ): Either[ComponentGatewayError, ImpalaEntityResource] =
    externalTableGateway
      .drop(
        connectionConfig,
        externalTable,
        ifExists = true
      )
      .leftMap(e => ComponentGatewayError(show"$e"))

  private def createAndGetExternalTable(
      env: Environment,
      opRequest: OutputPort[PublicImpalaTableCdw]
  ): Either[ComponentGatewayError, ImpalaEntityResource] = for {
    externalTable    <- ExternalTableMapper.map(opRequest.dataContract.schema, opRequest.specific)
    connectionConfig <- getConnectionConfig(env, opRequest)
    createdResource  <- createExternalTable(connectionConfig, externalTable)
  } yield createdResource

  private def refreshAndGetExternalTable(
      env: Environment,
      opRequest: OutputPort[PublicImpalaTableCdw]
  ): Either[ComponentGatewayError, ImpalaEntityResource] = for {
    externalTable    <- ExternalTableMapper.map(opRequest.dataContract.schema, opRequest.specific)
    connectionConfig <- getConnectionConfig(env, opRequest)
    createdResource  <- refreshExternalTable(connectionConfig, externalTable)
  } yield createdResource

  private def dropAndGetExternalTable(
      env: Environment,
      opRequest: OutputPort[PublicImpalaTableCdw]
  ): Either[ComponentGatewayError, ImpalaEntityResource] = for {
    externalTable    <- ExternalTableMapper.map(opRequest.dataContract.schema, opRequest.specific)
    connectionConfig <- getConnectionConfig(env, opRequest)
    droppedResource  <- dropExternalTable(connectionConfig, externalTable)
  } yield droppedResource

  private def getConnectionConfig(
      env: Environment,
      opRequest: OutputPort[PublicImpalaTableCdw]
  ): Either[ComponentGatewayError, ConnectionConfig] =
    for {
      impalaHost <- hostProvider
        .getImpalaCoordinatorHost(env, opRequest.specific.cdwVirtualWarehouse)
        .leftMap(e => ComponentGatewayError(show"$e"))
      connectionConfig <- ConnectionConfig
        .getFromConfig(
          ApplicationConfiguration.impalaConfig.getConfig(ApplicationConfiguration.JDBC_CONFIG),
          impalaHost)
        .leftMap(e => ComponentGatewayError(show"$e"))
    } yield connectionConfig
}
