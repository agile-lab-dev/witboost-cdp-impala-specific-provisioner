package it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.zone

import cats.implicits._
import it.agilelab.provisioning.commons.client.ranger.model.{ RangerSecurityZone, RangerService }
import it.agilelab.provisioning.commons.client.ranger.{ RangerClient, RangerClientError }
import it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.zone.RangerSecurityZoneGatewayError.{
  FindSecurityZoneOwnerErr,
  FindServiceErr,
  RangerSecurityZoneGatewayInitErr,
  UpsertSecurityZoneErr
}
import it.agilelab.provisioning.impala.table.provisioner.repository.RoleConfigRepository
import it.agilelab.provisioning.mesh.repository.Repository
import it.agilelab.provisioning.mesh.repository.RepositoryError.EntityDoesNotExists
import it.agilelab.provisioning.mesh.self.service.lambda.core.model.{ Domain, Role }
import it.agilelab.provisioning.mesh.self.service.lambda.core.repository.role.RoleDynamoDBRepository

class RangerSecurityZoneGateway(
    val roleRepository: Repository[Role, String, Unit],
    val rangerClient: RangerClient
) {

  /** Upserts a security zone based on the domain, the user and the service type. If a Security Zone already exists associated to the domain,
    * it is updated by including the received service type and folderUrl. Otherwise a new one it is created.
    *
    * The Security Zone will give access to all resources under the folder Url if isDestroy is false,
    * and related to the databases associated to the received domain ($domain_*)
    *
    * @param deployUser Deploy user to act as an admin user of the security zone
    * @param domain Domain that the security zone will be associated with.
    *               The Security Zone name and the owner role name will correspond to the domain short name
    * @param serviceType The service type related to the security zone
    * @param dlName Data lake name
    * @param folderUrl Location of the folder data to be included as part of the security zone
    * @param isDestroy Whether to include the received folder URL or to delete all folder references (both on creation and update)
    * @return Either a [[RangerSecurityZoneGatewayError]] error while upserting the security zone, or the newly created or updated [[RangerSecurityZone]]
    */
  def upsertSecurityZone(
      deployUser: String,
      domain: Domain,
      serviceType: String,
      dlName: String,
      folderUrl: Seq[String],
      isDestroy: Boolean = false
  ): Either[RangerSecurityZoneGatewayError, RangerSecurityZone] = for {
    zone <- rangerClient
      .findSecurityZoneByName(domain.shortName)
      .leftMap(e => UpsertSecurityZoneErr(e))
    service <- getService(serviceType, dlName)
    zoneUpdated <- upsertSC(
      domain.shortName,
      zone,
      service.name,
      folderUrl.map(u => safelyRemove(u, "/")),
      if (domain.name.equalsIgnoreCase("platform"))
        "platform-team-role"
      else s"${domain.name}-owner-role",
      deployUser,
      isDestroy
    )
  } yield zoneUpdated

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

  private def upsertSC(
      zoneName: String,
      zoneOpt: Option[RangerSecurityZone],
      serviceName: String,
      folderUrl: Seq[String],
      ownerRole: String,
      deployUser: String,
      isDestroy: Boolean
  ): Either[RangerSecurityZoneGatewayError, RangerSecurityZone] =
    zoneOpt.fold(
      createSecurityZone(
        zoneName,
        serviceName,
        if (isDestroy) Seq.empty[String] else folderUrl,
        ownerRole,
        deployUser))(z => updateSC(z, serviceName, folderUrl, isDestroy))

  private def createSecurityZone(
      zoneName: String,
      serviceName: String,
      folderUrl: Seq[String],
      ownerRole: String,
      deployUser: String
  ): Either[RangerSecurityZoneGatewayError, RangerSecurityZone] =
    roleRepository.findById(ownerRole) match {
      case Right(Some(u)) =>
        rangerClient
          .createSecurityZone(
            RangerSecurityZoneGenerator.securityZone(
              zoneName = zoneName,
              serviceName = serviceName,
              databaseResources = Seq(s"${zoneName}_*"),
              tableResources = Seq("*"),
              columnResources = Seq("*"),
              urlResources = folderUrl.map(u => s"$u/*"),
              adminUsers = Seq(deployUser),
              adminUserGroups = Seq(u.cdpRole),
              auditUsers = Seq(deployUser),
              auditUserGroups = Seq(u.cdpRole)
            ))
          .leftMap(e => UpsertSecurityZoneErr(e))
      case Right(None) =>
        Left(FindSecurityZoneOwnerErr(EntityDoesNotExists(ownerRole)))
      case Left(e) => Left(FindSecurityZoneOwnerErr(e))
    }

  private def updateSC(
      zone: RangerSecurityZone,
      serviceName: String,
      folderUrl: Seq[String],
      isDestroy: Boolean
  ): Either[RangerSecurityZoneGatewayError, RangerSecurityZone] =
    rangerClient
      .updateSecurityZone(
        if (isDestroy)
          RangerSecurityZoneGenerator.securityZoneWithoutUrlResources(
            zone,
            serviceName,
            Seq(s"${zone.name}_*"),
            Seq("*"),
            Seq("*"),
            folderUrl.map(u => s"$u/*")
          )
        else
          RangerSecurityZoneGenerator.securityZoneWithMergedServiceResources(
            zone,
            serviceName,
            Seq(s"${zone.name}_*"),
            Seq("*"),
            Seq("*"),
            folderUrl.map(u => s"$u/*")
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
      roleTable: String,
      roleTablePrimaryKey: String,
      rangerClient: RangerClient
  ): Either[RangerSecurityZoneGatewayError, RangerSecurityZoneGateway] = for {
    repo <- Repository
      .dynamoDB(roleTable, roleTablePrimaryKey, None)
      .leftMap(e => RangerSecurityZoneGatewayInitErr(e))
  } yield new RangerSecurityZoneGateway(
    new RoleDynamoDBRepository(repo),
    rangerClient
  )

  def defaultWithAudit(
      roleTable: String,
      roleTablePrimaryKey: String,
      rangerClient: RangerClient
  ): Either[RangerSecurityZoneGatewayError, RangerSecurityZoneGateway] = for {
    repo <- Repository
      .dynamoDBWithAudit(roleTable, roleTablePrimaryKey, None)
      .leftMap(e => RangerSecurityZoneGatewayInitErr(e))
  } yield new RangerSecurityZoneGateway(
    new RoleDynamoDBRepository(repo),
    rangerClient
  )

  def defaultWithConfig(rangerClient: RangerClient): RangerSecurityZoneGateway =
    new RangerSecurityZoneGateway(new RoleConfigRepository, rangerClient)
}
