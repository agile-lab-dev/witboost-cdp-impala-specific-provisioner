package it.agilelab.provisioning.impala.table.provisioner.gateway.resource

import cats.implicits._
import com.cloudera.cdp.datalake.model.Datalake
import com.cloudera.cdp.environments.model.Environment
import io.circe.Json
import it.agilelab.provisioning.impala.table.provisioner.clients.cdp.HostProvider
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider.ConnectionConfig
import it.agilelab.provisioning.impala.table.provisioner.core.model._
import it.agilelab.provisioning.impala.table.provisioner.gateway.mapper.ExternalTableMapper
import it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.provider.RangerGatewayProvider
import it.agilelab.provisioning.impala.table.provisioner.gateway.table.ExternalTableGateway
import it.agilelab.provisioning.mesh.self.service.api.model.Component.OutputPort
import it.agilelab.provisioning.mesh.self.service.api.model.ProvisionRequest
import it.agilelab.provisioning.mesh.self.service.core.gateway.{
  ComponentGateway,
  ComponentGatewayError
}
import it.agilelab.provisioning.mesh.self.service.core.model.ProvisionCommand
import it.agilelab.provisioning.mesh.self.service.lambda.core.model.Domain

class ImpalaTableOutputPortGateway(
    serviceRole: String,
    hostProvider: HostProvider,
    externalTableGateway: ExternalTableGateway,
    rangerGatewayProvider: RangerGatewayProvider
) extends ComponentGateway[Json, ImpalaCdw, ImpalaTableOutputPortResource] {

  /** Creates all the resources for the Output Port.
    *
    * It creates the Impala external table based on the location defined in the descriptor;
    * creates or updates the necessary security zones;
    * database, table and location policies for the defined owners and users in the component descriptor,
    * @param provisionCommand A Provision Command including the provisioning request containing the data product descriptor
    * @return Either a [[ComponentGatewayError]] if an error occurred while creating the output port,
    *         or an [[ImpalaTableOutputPortResource]] that includes the information of the newly deployed Output Port.
    */
  override def create(
      provisionCommand: ProvisionCommand[Json, ImpalaCdw]
  ): Either[ComponentGatewayError, ImpalaTableOutputPortResource] = for {
    opRequest <- getOutputPortRequest(provisionCommand.provisionRequest)
    env <- hostProvider
      .getEnvironment(opRequest.specific.cdpEnvironment)
      .leftMap(e => ComponentGatewayError(show"$e"))
    dl            <- hostProvider.getDataLake(env).leftMap(e => ComponentGatewayError(show"$e"))
    domain        <- Right(getDomain(provisionCommand.provisionRequest))
    externalTable <- createAndGetExternalTable(env, opRequest)
    gtwys         <- getRangerGateways(dl)
    _ <- gtwys._1
      .upsertSecurityZone(
        serviceRole,
        domain,
        "hive",
        dl.getDatalakeName,
        Seq(opRequest.specific.location)
      )
      .leftMap(e => ComponentGatewayError(show"$e"))
    policies <- gtwys._2
      .attachPolicy(
        externalTable.database,
        externalTable.tableName,
        externalTable.location,
        opRequest.specific.acl.owners,
        opRequest.specific.acl.users,
        serviceRole +: Seq(
          "hive",
          "beacon",
          "dpprofiler",
          "hue",
          "admin",
          "impala",
          "rangerlookup"),
        domain.shortName
      )
      .leftMap(e => ComponentGatewayError(show"$e"))
  } yield ImpalaTableOutputPortResource(
    externalTable,
    ImpalaCdpAcl(policies, Seq.empty[PolicyAttachment]))

  /** Destroys all the resources for the Output Port.
    *
    * It updates the security zones to remove the data location as part of the managed resources;
    * deletes the table and location policies for the defined owners and users in the component descriptor,
    *
    * @param unprovisionCommand A Provision Command including the unprovisioning request containing the data product descriptor
    * @return Either a [[ComponentGatewayError]] if an error occurred while destroying the output port,
    *         or an [[ImpalaTableOutputPortResource]] that includes the information of the deleted Output Port.
    */
  override def destroy(
      unprovisionCommand: ProvisionCommand[Json, ImpalaCdw]
  ): Either[ComponentGatewayError, ImpalaTableOutputPortResource] = for {
    opRequest <- getOutputPortRequest(unprovisionCommand.provisionRequest)
    env <- hostProvider
      .getEnvironment(opRequest.specific.cdpEnvironment)
      .leftMap(e => ComponentGatewayError(show"$e"))
    dl            <- hostProvider.getDataLake(env).leftMap(e => ComponentGatewayError(show"$e"))
    domain        <- Right(getDomain(unprovisionCommand.provisionRequest))
    externalTable <- createAndGetExternalTable(env, opRequest)
    gtwys         <- getRangerGateways(dl)
    _ <- gtwys._1
      .upsertSecurityZone(
        serviceRole,
        domain,
        "hive",
        dl.getDatalakeName,
        Seq(opRequest.specific.location),
        isDestroy = true
      )
      .leftMap(e => ComponentGatewayError(show"$e"))
    policies <- gtwys._2
      .detachPolicy(
        externalTable.database,
        externalTable.tableName,
        externalTable.location,
        opRequest.specific.acl.owners,
        opRequest.specific.acl.users,
        serviceRole +: Seq(
          "hive",
          "beacon",
          "dpprofiler",
          "hue",
          "admin",
          "impala",
          "rangerlookup"),
        domain.shortName
      )
      .leftMap(e => ComponentGatewayError(show"$e"))
  } yield ImpalaTableOutputPortResource(
    externalTable,
    ImpalaCdpAcl(Seq.empty[PolicyAttachment], policies))

  private def getOutputPortRequest(
      a: ProvisionRequest[Json, ImpalaCdw]
  ): Either[ComponentGatewayError, OutputPort[ImpalaCdw]] = a.component
    .toRight(ComponentGatewayError("Received provisioning request does not contain a component"))
    .flatMap {
      case c: OutputPort[ImpalaCdw] => Right(c)
      case _ =>
        Left(ComponentGatewayError("The provided component is not accepted by this provisioner"))
    }

  private def getDomain(req: ProvisionRequest[Json, ImpalaCdw]): Domain =
    Domain(req.dataProduct.domain, req.dataProduct.domain)

  private def createExternalTable(
      impalaHost: String,
      externalTable: ExternalTable
  ): Either[ComponentGatewayError, Unit] =
    externalTableGateway
      .create(
        ConnectionConfig(impalaHost, "443", "default", "", ""),
        externalTable,
        ifNotExists = true
      )
      .leftMap(e => ComponentGatewayError(show"$e"))

  private def getRangerGateways(dl: Datalake) = for {
    rangerHost <- hostProvider.getRangerHost(dl).leftMap(e => ComponentGatewayError(show"$e"))
    rangerClient <- rangerGatewayProvider
      .getRangerClient(rangerHost)
      .leftMap(e => ComponentGatewayError(show"$e"))
    rangerSecZoneGateway <- rangerGatewayProvider
      .getRangerSecurityZoneGateway(rangerClient)
      .leftMap(e => ComponentGatewayError(show"$e"))
    policyGateway <- rangerGatewayProvider
      .getRangerPolicyGateway(rangerClient)
      .leftMap(e => ComponentGatewayError(show"$e"))
  } yield (rangerSecZoneGateway, policyGateway)

  private def createAndGetExternalTable(env: Environment, opRequest: OutputPort[ImpalaCdw]) = for {
    impalaHost <- hostProvider
      .getImpalaCoordinatorHost(env, opRequest.specific.cdwVirtualWarehouse)
      .leftMap(e => ComponentGatewayError(show"$e"))
    externalTable <- ExternalTableMapper.map(opRequest)
    _             <- createExternalTable(impalaHost, externalTable)
  } yield externalTable

}
