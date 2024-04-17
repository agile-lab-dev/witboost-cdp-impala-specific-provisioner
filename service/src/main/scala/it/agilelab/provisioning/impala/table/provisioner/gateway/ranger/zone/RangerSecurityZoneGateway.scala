package it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.zone

import cats.implicits._
import it.agilelab.provisioning.commons.client.ranger.model.{ RangerSecurityZone, RangerService }
import it.agilelab.provisioning.commons.client.ranger.{ RangerClient, RangerClientError }
import it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.zone.RangerSecurityZoneGatewayError.{
  FindServiceErr,
  UpsertSecurityZoneErr
}

class RangerSecurityZoneGateway(
    val rangerClient: RangerClient
) {

  /** Upserts a security zone based on the domain, the user and the service type. If a Security Zone already exists associated to the domain,
    * it is updated by including the received service type and folderUrl. Otherwise a new one it is created.
    *
    * The Security Zone will give access to all resources under the folder Url if isDestroy is false,
    * and related to the databases associated to the received domain ($domain_*)
    *
    * @param deployUser Deploy user to act as an admin user of the security zone
    * @param securityZoneName Security Zone name
    * @param auditUsers List of users to be added as part of the audits of the Security Zone
    * @param auditGroups List of groups to be added as part of the audit groups of the Security Zone
    * @param serviceType The service type related to the security zone
    * @param clusterName Cluster name
    * @param databaseName Database name
    * @param folderUrl Location of the folder data to be included as part of the security zone
    * @param isDestroy Whether to include the received folder URL or to delete all folder references (both on creation and update)
    * @return Either a [[RangerSecurityZoneGatewayError]] error while upserting the security zone, or the newly created or updated [[RangerSecurityZone]]
    */
  def upsertSecurityZone(
      deployUser: String,
      securityZoneName: String,
      auditUsers: Seq[String],
      auditGroups: Seq[String],
      serviceType: String,
      clusterName: String,
      databaseName: String,
      folderUrl: Seq[String],
      isDestroy: Boolean = false
  ): Either[RangerSecurityZoneGatewayError, RangerSecurityZone] =
    RangerSecurityZoneGateway.synchronized {
      for {
        zoneOpt <- rangerClient
          .findSecurityZoneByName(securityZoneName)
          .leftMap(e => UpsertSecurityZoneErr(e))
        service <- getService(serviceType, clusterName)
        zoneUpdated <- zoneOpt.fold(
          createSecurityZone(
            securityZoneName,
            service.name,
            databaseName,
            if (isDestroy) Seq.empty[String] else folderUrl.map(u => safelyRemove(u, "/")),
            auditUsers,
            auditGroups,
            deployUser))(z => updateSC(z, service.name, databaseName, folderUrl, isDestroy))
      } yield zoneUpdated
    }

  private def getService(
      serviceType: String,
      dlName: String
  ): Either[RangerSecurityZoneGatewayError, RangerService] = for {
    services <- findServicesByTypeInCluster(serviceType, dlName)
      .leftMap(e => FindServiceErr(e.toString))
    s <- services.headOption.toRight[FindServiceErr](
      FindServiceErr(
        "Unable to find service with " +
          "type %s in cluster %s.".format(serviceType, dlName)))
  } yield s

  private def createSecurityZone(
      zoneName: String,
      serviceName: String,
      databaseName: String,
      folderUrl: Seq[String],
      auditUsers: Seq[String],
      auditGroups: Seq[String],
      deployUser: String
  ): Either[RangerSecurityZoneGatewayError, RangerSecurityZone] =
    rangerClient
      .createSecurityZone(
        RangerSecurityZoneGenerator.securityZone(
          zoneName = zoneName,
          serviceName = serviceName,
          databaseResources = Seq(s"${databaseName}*"),
          tableResources = Seq("*"),
          columnResources = Seq("*"),
          urlResources = folderUrl.map(u => s"$u/*"),
          adminUsers = Seq(deployUser),
          adminUserGroups = Seq.empty,
          auditUsers = deployUser +: auditUsers,
          auditUserGroups = auditGroups
        ))
      .leftMap(e => UpsertSecurityZoneErr(e))

  private def updateSC(
      zone: RangerSecurityZone,
      serviceName: String,
      databaseName: String,
      folderUrl: Seq[String],
      isDestroy: Boolean
  ): Either[RangerSecurityZoneGatewayError, RangerSecurityZone] =
    rangerClient
      .updateSecurityZone(
        if (isDestroy)
          RangerSecurityZoneGenerator.securityZoneWithoutUrlResources(
            zone,
            serviceName,
            Seq(s"${databaseName}*"),
            Seq("*"),
            Seq("*"),
            folderUrl.map(u => s"${safelyRemove(u, "/")}/*")
          )
        else
          RangerSecurityZoneGenerator.securityZoneWithMergedServiceResources(
            zone,
            serviceName,
            Seq(s"${databaseName}*"),
            Seq("*"),
            Seq("*"),
            folderUrl.map(u => s"${safelyRemove(u, "/")}/*")
          )
      )
      .leftMap(e => UpsertSecurityZoneErr(e))

  private def safelyRemove(value: String, suffix: String): String =
    if (suffix.length <= value.length && value.endsWith(suffix))
      value.substring(0, value.length - suffix.length)
    else value

  private def findServicesByTypeInCluster(
      serviceType: String,
      clusterName: String
  ): Either[RangerClientError, Seq[RangerService]] =
    for {
      services <- rangerClient.findAllServices
      servicesFiltered = services.filter(s =>
        s.`type`.equalsIgnoreCase(serviceType) &&
        s.configs.getOrElse("cluster.name", "") === clusterName)
    } yield servicesFiltered

}

object RangerSecurityZoneGateway {
  def default(
      rangerClient: RangerClient
  ): RangerSecurityZoneGateway =
    new RangerSecurityZoneGateway(rangerClient)
}
