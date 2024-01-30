package it.agilelab.provisioning.impala.table.provisioner.gateway.resource

import cats.implicits.{ showInterpolator, toBifunctorOps }
import io.circe.Json
import it.agilelab.provisioning.commons.client.ranger.RangerClient
import it.agilelab.provisioning.commons.client.ranger.model.RangerRole
import it.agilelab.provisioning.commons.principalsmapping.{
  CdpIamGroup,
  CdpIamPrincipals,
  CdpIamUser,
  PrincipalsMapper
}
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaCdw.ImpalaProvisionRequestOps
import it.agilelab.provisioning.impala.table.provisioner.core.model.{
  ExternalTable,
  ImpalaCdw,
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
import it.agilelab.provisioning.mesh.self.service.api.model.Component.OutputPort
import it.agilelab.provisioning.mesh.self.service.api.model.{ DataProduct, ProvisionRequest }
import it.agilelab.provisioning.mesh.self.service.core.gateway.ComponentGatewayError

class ImpalaOutputPortAccessControlGateway(
    serviceRole: String,
    rangerGatewayProvider: RangerGatewayProvider,
    principalsMapper: PrincipalsMapper[CdpIamPrincipals]
) {

  /** Provisions the necessary access control entities for managing the provision request. For Output Ports, it provisions:
    *
    * - A security zone at DP level
    * - Two roles, owner role (at DP level) and user role (at component level).
    *   The dataProductOwner and developmentGroup are assigned to the owner role.
    * - Two access policies, for the database and the tables.
    *   These access policies grant read-write permissions to the owner role, and read-only to the user role
    *   The operation is idempotent, so if any of these resources exist, they're not recreated
    * @param provisionRequest Provision request with the output port information
    * @param rangerClient Ranger Client to be used for contacting Ranger
    * @param externalTable External table to which access will be managed
    * @param clusterName Cluster name used to create Security Zones on Ranger. On CDP Public it refers to the Datalake name
    * @return Either a [[ComponentGatewayError]] if an error occurs, or a sequence of [[PolicyAttachment]] with the access policies
    */
  def provisionAccessControl(
      provisionRequest: ProvisionRequest[Json, Json],
      rangerClient: RangerClient,
      externalTable: ExternalTable,
      clusterName: String
  ): Either[ComponentGatewayError, Seq[PolicyAttachment]] =
    for {
      opRequest <- provisionRequest
        .getOutputPortRequest[ImpalaCdw]
        .leftMap(e => ComponentGatewayError(show"$e"))
      identifiers <- extractIdentifiers(opRequest)
      szName <- Right(
        RangerSecurityZoneGenerator
          .generateSecurityZoneName(identifiers._1, identifiers._2, identifiers._3))
      owners <- getOwnerPrincipals(provisionRequest.dataProduct)
      gtwys  <- getRangerGateways(rangerClient)
      securityZone <- gtwys.securityZoneGateway
        .upsertSecurityZone(
          serviceRole,
          szName,
          owners._1.workloadUsername,
          Some(owners._2.groupName),
          "hive",
          clusterName,
          Seq(opRequest.specific.location)
        )
        .leftMap(e => ComponentGatewayError(show"$e"))
      ownerRole <- gtwys.roleGateway
        .upsertRole(
          rolePrefix = s"${identifiers._1}_${identifiers._2}_${identifiers._3}",
          OwnerRoleType,
          serviceRole,
          ownerUsers = List.empty,
          ownerGroups = List.empty,
          users = List(owners._1.workloadUsername),
          groups = List(owners._2.groupName)
        )
        .leftMap(e => ComponentGatewayError(show"$e"))
      userRole <- gtwys.roleGateway
        .upsertRole(
          rolePrefix = s"${identifiers._1}_${identifiers._2}_${identifiers._3}_${identifiers._4}",
          UserRoleType,
          serviceRole,
          ownerUsers = List.empty,
          ownerGroups = List.empty,
          users = List.empty,
          groups = List.empty
        )
        .leftMap(e => ComponentGatewayError(show"$e"))
      policies <- gtwys.policyGateway
        .upsertPolicies(
          externalTable.database,
          externalTable.tableName,
          externalTable.location,
          ownerRole.name,
          userRole.name,
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
    * @param provisionRequest Provision request with the output port information
    * @param rangerClient      Ranger Client to be used for contacting Ranger
    * @param externalTable     External table to which access will be managed
    * @param clusterName       Cluster name used to update Security Zones on Ranger. On CDP Public it refers to the Datalake name
    * @return Either a [[ComponentGatewayError]] if an error occurs, or a sequence of [[PolicyAttachment]] with the unprovisioned access policies
    */

  def unprovisionAccessControl(
      provisionRequest: ProvisionRequest[Json, Json],
      rangerClient: RangerClient,
      externalTable: ExternalTable,
      clusterName: String
  ): Either[ComponentGatewayError, Seq[PolicyAttachment]] = for {
    opRequest <- provisionRequest
      .getOutputPortRequest[ImpalaCdw]
      .leftMap(e => ComponentGatewayError(show"$e"))
    identifiers <- extractIdentifiers(opRequest)
    szName <- Right(
      RangerSecurityZoneGenerator
        .generateSecurityZoneName(identifiers._1, identifiers._2, identifiers._3))
    owners <- getOwnerPrincipals(provisionRequest.dataProduct)
    gtwys  <- getRangerGateways(rangerClient)
    userRoleName = RangerRoleGenerator.generateUserRoleName(
      s"${identifiers._1}_${identifiers._2}_${identifiers._3}_${identifiers._4}")
    securityZone <- gtwys.securityZoneGateway
      .upsertSecurityZone(
        serviceRole,
        szName,
        owners._1.workloadUsername,
        Some(owners._2.groupName),
        "hive",
        clusterName,
        Seq(opRequest.specific.location),
        isDestroy = true
      )
      .leftMap(e => ComponentGatewayError(show"$e"))
    policies <- gtwys.policyGateway
      .deletePolicies(
        externalTable.database,
        externalTable.tableName,
        externalTable.location,
        userRoleName,
        securityZone.name
      )
      .leftMap(e => ComponentGatewayError(show"$e"))
    _ <- gtwys.roleGateway
      .deleteUserRole(s"${identifiers._1}_${identifiers._2}_${identifiers._3}_${identifiers._4}")
      .leftMap(e => ComponentGatewayError(show"$e"))
  } yield policies

  def updateAcl(
      provisionRequest: ProvisionRequest[Json, Json],
      refs: Set[CdpIamPrincipals],
      rangerClient: RangerClient
  ): Either[ComponentGatewayError, RangerRole] = for {
    opRequest <- provisionRequest
      .getOutputPortRequest[ImpalaCdw]
      .leftMap(e => ComponentGatewayError(show"$e"))
    identifiers <- extractIdentifiers(opRequest)
    gtwys       <- getRangerGateways(rangerClient)
    usersGroups <- Right(refs.partitionMap {
      case CdpIamUser(_, workloadUsername, _) => Left(workloadUsername)
      case CdpIamGroup(groupName, _)          => Right(groupName)
    })
    userRole <- gtwys.roleGateway
      .upsertRole(
        rolePrefix = s"${identifiers._1}_${identifiers._2}_${identifiers._3}_${identifiers._4}",
        UserRoleType,
        serviceRole,
        ownerUsers = List.empty,
        ownerGroups = List.empty,
        users = usersGroups._1.toList,
        groups = usersGroups._2.toList
      )
      .leftMap(e => ComponentGatewayError(show"$e"))
  } yield userRole

  private def extractIdentifiers(opRequest: OutputPort[_]) =
    opRequest.id match {
      case s"urn:dmb:cmp:$domain:$name:$majorVersion:$componentName" =>
        Right((domain, name, majorVersion, componentName))
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
    * @param dataProduct Data product with the owner information
    * @return Either an error if the user and group are not correctly mapped, or a Right with a tuple with the mapped User and Group
    */
  private def getOwnerPrincipals(
      dataProduct: DataProduct[Json]
  ): Either[ComponentGatewayError, (CdpIamUser, CdpIamGroup)] = for {
    owners <- Right(principalsMapper.map(Set(dataProduct.dataProductOwner, dataProduct.devGroup)))
    ownerUser <- owners.get(dataProduct.dataProductOwner) match {
      case Some(Right(principal: CdpIamUser)) => Right(principal)
      case Some(Right(_))                     => Left(ComponentGatewayError("Data product owner is not a CDP user"))
      case Some(Left(err))                    => Left(ComponentGatewayError(show"$err"))
      case None =>
        Left(
          ComponentGatewayError("Something went wrong while retrieving mapped data product owner")
        )
    }
    ownerGroup <- owners.get(dataProduct.devGroup) match {
      case Some(Right(principal: CdpIamGroup)) => Right(principal)
      case Some(Right(_))                      => Left(ComponentGatewayError("Development group is not a CDP group"))
      case Some(Left(err))                     => Left(ComponentGatewayError(show"$err"))
      case None =>
        Left(
          ComponentGatewayError("Something went wrong while retrieving mapped development group")
        )
    }
  } yield (ownerUser, ownerGroup)
}
