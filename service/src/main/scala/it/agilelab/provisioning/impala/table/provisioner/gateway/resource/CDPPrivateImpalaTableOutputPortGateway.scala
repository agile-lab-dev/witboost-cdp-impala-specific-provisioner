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
import it.agilelab.provisioning.impala.table.provisioner.gateway.mapper.ExternalTableMapper
import it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.provider.RangerGatewayProvider
import it.agilelab.provisioning.impala.table.provisioner.gateway.table.ExternalTableGateway
import it.agilelab.provisioning.mesh.self.service.api.model.Component.OutputPort
import it.agilelab.provisioning.mesh.self.service.core.gateway.{
  ComponentGateway,
  ComponentGatewayError
}
import it.agilelab.provisioning.mesh.self.service.core.model.ProvisionCommand

import scala.util.Try

class CDPPrivateImpalaTableOutputPortGateway(
    serviceRole: String,
    hostProvider: ConfigHostProvider,
    externalTableGateway: ExternalTableGateway,
    rangerGatewayProvider: RangerGatewayProvider,
    impalaAccessControlGateway: ImpalaOutputPortAccessControlGateway
) extends ComponentGateway[Json, Json, ImpalaTableOutputPortResource, CdpIamPrincipals] {

  /** Creates all the resources for the Output Port.
    *
    * It creates the Impala external table based on the location defined in the descriptor;
    * creates or updates the necessary security zones;
    * database, table and location policies for the defined owners and users in the component descriptor,
    *
    * @param provisionCommand A Provision Command including the provisioning request containing the data product descriptor
    * @return Either a [[ComponentGatewayError]] if an error occurred while creating the output port,
    *         or an [[ImpalaTableOutputPortResource]] that includes the information of the newly deployed Output Port.
    */
  override def create(
      provisionCommand: ProvisionCommand[Json, Json]
  ): Either[ComponentGatewayError, ImpalaTableOutputPortResource] = for {
    // Extract necessary information
    opRequest <- provisionCommand.provisionRequest
      .getOutputPortRequest[PrivateImpalaCdw]
      .leftMap(e => ComponentGatewayError(show"$e"))
    clusterName <- Try(
      ApplicationConfiguration.provisionerConfig.getString(
        ApplicationConfiguration.PROVISION_CLUSTER_NAME)).toEither
      .leftMap { _ =>
        val confError = ConfKeyNotFoundErr(ApplicationConfiguration.PROVISION_CLUSTER_NAME)
        ComponentGatewayError(show"$confError")
      }
    // Upsert output port
    externalTable <- createAndGetExternalTable(opRequest)
    // Upsert security and access
    rangerHost <- hostProvider.getRangerHost
      .leftMap(e => ComponentGatewayError(show"$e"))
    rangerClient <- rangerGatewayProvider
      .getRangerClient(rangerHost)
      .leftMap(e => ComponentGatewayError(show"$e"))
    policies <- impalaAccessControlGateway.provisionAccessControl(
      provisionCommand.provisionRequest,
      rangerClient,
      externalTable,
      clusterName
    )
  } yield ImpalaTableOutputPortResource(
    externalTable,
    ImpalaCdpAcl(policies, Seq.empty[PolicyAttachment]))

  override def destroy(
      unprovisionCommand: ProvisionCommand[Json, Json]
  ): Either[ComponentGatewayError, ImpalaTableOutputPortResource] = for {
    // Extract necessary information
    opRequest <- unprovisionCommand.provisionRequest
      .getOutputPortRequest[PrivateImpalaCdw]
      .leftMap(e => ComponentGatewayError(show"$e"))
    dropOnUnprovision <- Try {
      ApplicationConfiguration.impalaConfig.getBoolean(ApplicationConfiguration.DROP_ON_UNPROVISION)
    }.toEither.leftMap { _ =>
      val confError = ConfKeyNotFoundErr(ApplicationConfiguration.DROP_ON_UNPROVISION)
      ComponentGatewayError(show"$confError")
    }
    clusterName <- Try(
      ApplicationConfiguration.provisionerConfig.getString(
        ApplicationConfiguration.PROVISION_CLUSTER_NAME)).toEither
      .leftMap { _ =>
        val confError = ConfKeyNotFoundErr(ApplicationConfiguration.PROVISION_CLUSTER_NAME)
        ComponentGatewayError(show"$confError")
      }
    // Upsert/delete output port
    externalTable <-
      if (dropOnUnprovision) {
        dropAndGetExternalTable(opRequest)
      } else {
        createAndGetExternalTable(opRequest)
      }
    // Update security and access
    rangerHost <- hostProvider.getRangerHost
      .leftMap(e => ComponentGatewayError(show"$e"))
    rangerClient <- rangerGatewayProvider
      .getRangerClient(rangerHost)
      .leftMap(e => ComponentGatewayError(show"$e"))
    policies <- impalaAccessControlGateway.unprovisionAccessControl(
      unprovisionCommand.provisionRequest,
      rangerClient,
      externalTable,
      clusterName
    )
  } yield ImpalaTableOutputPortResource(
    externalTable,
    ImpalaCdpAcl(Seq.empty[PolicyAttachment], policies))

  override def updateAcl(
      provisionCommand: ProvisionCommand[Json, Json],
      refs: Set[CdpIamPrincipals]
  ): Either[ComponentGatewayError, Set[CdpIamPrincipals]] = for {
    _ <- provisionCommand.provisionRequest
      .getOutputPortRequest[PrivateImpalaCdw]
      .leftMap(e => ComponentGatewayError(show"$e"))
    rangerHost <- hostProvider.getRangerHost
      .leftMap(e => ComponentGatewayError(show"$e"))
    rangerClient <- rangerGatewayProvider
      .getRangerClient(rangerHost)
      .leftMap(e => ComponentGatewayError(show"$e"))
    _ <- impalaAccessControlGateway.updateAcl(provisionCommand.provisionRequest, refs, rangerClient)
  } yield refs

  private def createExternalTable(
      connectionConfig: ConnectionConfig,
      externalTable: ExternalTable
  ): Either[ComponentGatewayError, Unit] =
    externalTableGateway
      .create(
        connectionConfig,
        externalTable,
        ifNotExists = true
      )
      .leftMap(e => ComponentGatewayError(show"$e"))

  private def dropExternalTable(
      connectionConfig: ConnectionConfig,
      externalTable: ExternalTable
  ): Either[ComponentGatewayError, Unit] =
    externalTableGateway
      .drop(
        connectionConfig,
        externalTable,
        ifExists = true
      )
      .leftMap(e => ComponentGatewayError(show"$e"))

  private def createAndGetExternalTable(
      opRequest: OutputPort[PrivateImpalaCdw]
  ): Either[ComponentGatewayError, ExternalTable] = for {
    impalaHost <- hostProvider
      .getImpalaCoordinatorHost()
      .leftMap(e => ComponentGatewayError(show"$e"))
    externalTable <- ExternalTableMapper.map(opRequest.dataContract.schema, opRequest.specific)
    connectionConfig <- ConnectionConfig
      .getFromConfig(ApplicationConfiguration.impalaConfig, impalaHost)
      .leftMap(e => ComponentGatewayError(show"$e"))
    _ <- createExternalTable(connectionConfig, externalTable)
  } yield externalTable

  private def dropAndGetExternalTable(
      opRequest: OutputPort[PrivateImpalaCdw]
  ): Either[ComponentGatewayError, ExternalTable] = for {
    impalaHost <- hostProvider
      .getImpalaCoordinatorHost()
      .leftMap(e => ComponentGatewayError(show"$e"))
    externalTable <- ExternalTableMapper.map(opRequest.dataContract.schema, opRequest.specific)
    connectionConfig <- ConnectionConfig
      .getFromConfig(ApplicationConfiguration.impalaConfig, impalaHost)
      .leftMap(e => ComponentGatewayError(show"$e"))
    _ <- dropExternalTable(connectionConfig, externalTable)
  } yield externalTable
}
