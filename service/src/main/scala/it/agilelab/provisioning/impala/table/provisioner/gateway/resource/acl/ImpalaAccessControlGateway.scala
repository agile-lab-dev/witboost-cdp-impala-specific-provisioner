package it.agilelab.provisioning.impala.table.provisioner.gateway.resource.acl

import cats.implicits.{ catsSyntaxEitherId, showInterpolator, toBifunctorOps }
import it.agilelab.provisioning.commons.client.ranger.RangerClient
import it.agilelab.provisioning.commons.client.ranger.model.RangerRole
import it.agilelab.provisioning.commons.principalsmapping.{
  CdpIamGroup,
  CdpIamPrincipals,
  CdpIamUser,
  PrincipalsMapper
}
import it.agilelab.provisioning.impala.table.provisioner.core.model.{
  ExternalTable,
  PolicyAttachment
}
import it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.provider.{
  RangerGateway,
  RangerGatewayProvider
}
import it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.role.{
  OwnerRoleType,
  RangerRoleGenerator,
  UserRoleType
}
import it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.zone.RangerSecurityZoneGenerator
import it.agilelab.provisioning.mesh.self.service.core.gateway.ComponentGatewayError

class ImpalaAccessControlGateway(
    serviceRole: String,
    rangerGatewayProvider: RangerGatewayProvider,
    principalsMapper: PrincipalsMapper[CdpIamPrincipals]
) {

  /** Provisions the necessary access control entities for managing the provision request. For Output Ports, it provisions:
    *
    * - A security zone at DP level
    * - Two roles, owner role (at DP level) and user role* (at component level).
    *   The dataProductOwner and developmentGroup are assigned to the owner role.
    * - Two access policies, for the database and the tables.
    *   These access policies grant read-write permissions to the owner role, and read-only to the user role
    *   The operation is idempotent, so if any of these resources exist, they're not recreated
    * @param accessControlInfo Information of the data product and component to be provisioned required to provision the necessary access control resources
    * @param rangerClient Ranger Client to be used for contacting Ranger
    * @param externalTable External table to which access will be managed
    * @param clusterName Cluster name used to create Security Zones on Ranger. On CDP Public it refers to the Datalake name
    * @param provisionUserRole Whether to provision or not a user role (useful for storage components) TODO Improve this so StorageArea use a specific gateway
    * @return Either a [[ComponentGatewayError]] if an error occurs, or a sequence of [[PolicyAttachment]] with the access policies
    */
  def provisionAccessControl(
      accessControlInfo: AccessControlInfo,
      rangerClient: RangerClient,
      externalTable: ExternalTable,
      clusterName: String,
      provisionUserRole: Boolean
  ): Either[ComponentGatewayError, Seq[PolicyAttachment]] =
    for {
      identifiers <- extractIdentifiers(accessControlInfo.componentId)
      szName <- Right(
        RangerSecurityZoneGenerator
          .generateSecurityZoneName(
            identifiers.domain,
            identifiers.dataProductName,
            identifiers.dataProductMajorVersion))
      owners <- getOwnerPrincipals(accessControlInfo.dataProductOwner, accessControlInfo.devGroup)
      gtwys  <- getRangerGateways(rangerClient)
      securityZone <- gtwys.securityZoneGateway
        .upsertSecurityZone(
          serviceRole,
          szName,
          owners._1.workloadUsername,
          Some(owners._2.groupName),
          "hive",
          clusterName,
          Seq(externalTable.location)
        )
        .leftMap(e => ComponentGatewayError(show"$e"))
      ownerRole <- gtwys.roleGateway
        .upsertRole(
          rolePrefix =
            s"${identifiers.domain}_${identifiers.dataProductName}_${identifiers.dataProductMajorVersion}",
          OwnerRoleType,
          serviceRole,
          ownerUsers = List.empty,
          ownerGroups = List.empty,
          users = List(owners._1.workloadUsername),
          groups = List(owners._2.groupName)
        )
        .leftMap(e => ComponentGatewayError(show"$e"))
      userRole <-
        if (provisionUserRole)
          gtwys.roleGateway
            .upsertRole(
              rolePrefix =
                s"${identifiers.domain}_${identifiers.dataProductName}_${identifiers.dataProductMajorVersion}_${identifiers.componentName}",
              UserRoleType,
              serviceRole,
              ownerUsers = List.empty,
              ownerGroups = List.empty,
              users = List.empty,
              groups = List.empty
            )
            .map(Some(_))
            .leftMap(e => ComponentGatewayError(show"$e"))
        else Right(None)
      policies <- gtwys.policyGateway
        .upsertPolicies(
          externalTable.database,
          externalTable.tableName,
          externalTable.location,
          ownerRole.name,
          userRole.map(_.name),
          serviceRole +: Seq(
            "hive",
            "beacon",
            "dpprofiler",
            "hue",
            "admin",
            "impala",
            "rangerlookup"),
          securityZone.name
        )
        .leftMap(e => ComponentGatewayError(show"$e"))
    } yield policies

  /** Unprovisions the access control entities that manage the provision request. For Output Ports, it unprovisions:
    * - One role, the user role
    * - Two access policies, for the database and the tables.
    * The operation is idempotent, so if any of these resources exist, they're not recreated
    *
    * @param accessControlInfo Information of the data product and component to be provisioned required to unprovision the necessary access control resources
    * @param rangerClient      Ranger Client to be used for contacting Ranger
    * @param externalTable     External table to which access will be managed
    * @param clusterName       Cluster name used to update Security Zones on Ranger. On CDP Public it refers to the Datalake name
    * @param unprovisionUserRole Whether to unprovision or not a user role (useful for storageareas) TODO Improve this so Storagearea use a specific gateway
    * @return Either a [[ComponentGatewayError]] if an error occurs, or a sequence of [[PolicyAttachment]] with the unprovisioned access policies
    */

  def unprovisionAccessControl(
      accessControlInfo: AccessControlInfo,
      rangerClient: RangerClient,
      externalTable: ExternalTable,
      clusterName: String,
      unprovisionUserRole: Boolean
  ): Either[ComponentGatewayError, Seq[PolicyAttachment]] = for {
    identifiers <- extractIdentifiers(accessControlInfo.componentId)
    szName <- Right(
      RangerSecurityZoneGenerator
        .generateSecurityZoneName(
          identifiers.domain,
          identifiers.dataProductName,
          identifiers.dataProductMajorVersion))
    owners <- getOwnerPrincipals(accessControlInfo.dataProductOwner, accessControlInfo.devGroup)
    gtwys  <- getRangerGateways(rangerClient)
    securityZone <- gtwys.securityZoneGateway
      .upsertSecurityZone(
        serviceRole,
        szName,
        owners._1.workloadUsername,
        Some(owners._2.groupName),
        "hive",
        clusterName,
        Seq(externalTable.location),
        isDestroy = true
      )
      .leftMap(e => ComponentGatewayError(show"$e"))
    userRoleName = Option.when(unprovisionUserRole)(RangerRoleGenerator.generateUserRoleName(
      s"${identifiers.domain}_${identifiers.dataProductName}_${identifiers.dataProductMajorVersion}_${identifiers.componentName}"))
    policies <- gtwys.policyGateway
      .deletePolicies(
        externalTable.database,
        externalTable.tableName,
        externalTable.location,
        userRoleName,
        securityZone.name
      )
      .leftMap(e => ComponentGatewayError(show"$e"))
    _ <-
      if (unprovisionUserRole) {
        gtwys.roleGateway
          .deleteUserRole(
            s"${identifiers.domain}_${identifiers.dataProductName}_${identifiers.dataProductMajorVersion}_${identifiers.componentName}")
          .leftMap(e => ComponentGatewayError(show"$e"))
      } else Right(())
  } yield policies

  /** Updates the access control entities that manage the provision request to allow access to a set of principals. For Output Ports, it updates the User role
    * The operation is idempotent, so if any of these resources exist, they're not recreated
    * @param accessControlInfo   Information of the data product and component required to update the necessary access control resources
    * @param refs Set of principals to put on the Ranger Role. The principals already present on the role will be replaced by these ones, with the exception of admin principals
    * @param rangerClient        Ranger Client to be used for contacting Ranger
    * @return Either a [[ComponentGatewayError]] if an error occurs, or a sequence of [[PolicyAttachment]] with the unprovisioned access policies
    */
  def updateAcl(
      accessControlInfo: AccessControlInfo,
      refs: Set[CdpIamPrincipals],
      rangerClient: RangerClient
  ): Either[ComponentGatewayError, RangerRole] = for {
    identifiers <- extractIdentifiers(accessControlInfo.componentId)
    gtwys       <- getRangerGateways(rangerClient)
    usersGroups <- Right(refs.partitionMap {
      case CdpIamUser(_, workloadUsername, _) => Left(workloadUsername)
      case CdpIamGroup(groupName, _)          => Right(groupName)
    })
    userRole <- gtwys.roleGateway
      .upsertRole(
        rolePrefix =
          s"${identifiers.domain}_${identifiers.dataProductName}_${identifiers.dataProductMajorVersion}_${identifiers.componentName}",
        UserRoleType,
        serviceRole,
        ownerUsers = List.empty,
        ownerGroups = List.empty,
        users = usersGroups._1.toList,
        groups = usersGroups._2.toList
      )
      .leftMap(e => ComponentGatewayError(show"$e"))
  } yield userRole

  private def extractIdentifiers(
      componentId: String
  ): Either[ComponentGatewayError, ComponentInfo] =
    componentId match {
      case s"urn:dmb:cmp:$domain:$name:$majorVersion:$componentName" =>
        Right(ComponentInfo(domain, name, majorVersion, componentName))
      case _ =>
        Left(
          ComponentGatewayError(
            "Component id is not in the expected shape, cannot extract attributes"
          )
        )
    }

  private def getRangerGateways(
      rangerClient: RangerClient
  ): Either[ComponentGatewayError, RangerGateway] = for {
    rangerGateways <- rangerGatewayProvider
      .getRangerGatewaysFromClient(rangerClient)
      .leftMap(e => ComponentGatewayError(show"$e"))
  } yield rangerGateways

  /** Maps dataProductOwner and devGroup to CdpIam user and groups
    *
    * @param dataProductOwner Owner of the data product. It should be an User in the form expected by the PrincipalsMapper
    * @param devGroup Development group of the data product. It should be a Group in the form expected by the PrincipalsMapper
    * @return Either an error if the user and group are not correctly mapped, or a Right with a tuple with the mapped User and Group
    */
  private def getOwnerPrincipals(
      dataProductOwner: String,
      devGroup: String
  ): Either[ComponentGatewayError, (CdpIamUser, CdpIamGroup)] = for {
    owners <- Right(principalsMapper.map(Set(dataProductOwner, devGroup)))
    ownerUser <- owners.get(dataProductOwner) match {
      case Some(Right(principal: CdpIamUser)) => Right(principal)
      case Some(Right(_))                     => Left(ComponentGatewayError("Data product owner is not a CDP user"))
      case Some(Left(err))                    => Left(ComponentGatewayError(show"$err"))
      case None =>
        Left(
          ComponentGatewayError("Something went wrong while retrieving mapped data product owner")
        )
    }
    ownerGroup <- owners.get(devGroup) match {
      case Some(Right(principal: CdpIamGroup)) => Right(principal)
      case Some(Right(_))                      => Left(ComponentGatewayError("Development group is not a CDP group"))
      case Some(Left(err))                     => Left(ComponentGatewayError(show"$err"))
      case None =>
        Left(
          ComponentGatewayError("Something went wrong while retrieving mapped development group")
        )
    }
  } yield (ownerUser, ownerGroup)

  final case class ComponentInfo(
      domain: String,
      dataProductName: String,
      dataProductMajorVersion: String,
      componentName: String
  )

}
