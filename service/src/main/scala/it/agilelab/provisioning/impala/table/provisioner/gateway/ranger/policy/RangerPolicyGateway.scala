package it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.policy

import cats.implicits._
import it.agilelab.provisioning.commons.client.ranger.RangerClient
import it.agilelab.provisioning.commons.client.ranger.model.RangerPolicy
import it.agilelab.provisioning.impala.table.provisioner.core.model.PolicyAttachment
import it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.policy.RangerPolicyGatewayError.{
  AttachRangerPolicyErr,
  RangerPolicyGatewayInitErr,
  UpsertPolicyErr
}
import it.agilelab.provisioning.impala.table.provisioner.repository.RoleConfigRepository
import it.agilelab.provisioning.mesh.repository.{ Repository, RepositoryError }
import it.agilelab.provisioning.mesh.self.service.lambda.core.model.Role
import it.agilelab.provisioning.mesh.self.service.lambda.core.repository.role.RoleDynamoDBRepository

class RangerPolicyGateway(
    roleRepository: Repository[Role, String, Unit],
    rangerClient: RangerClient
) {

  /** Upserts a set of policies to allow access to a specific database, table and url to a set of owners and users.
    * If the policy already exists for the database and/or table, it is updated with the received parameters.
    * Otherwise, a new one is created.
    * @param database Database name. It is used to define the database, table, and url policy name
    * @param table Table name. It is used to define the table policy name and the url policy name
    * @param url Location of the data
    * @param owners List of users to act as owners of the resources, having read/write access. These will be mapped to CDP roles using the RoleRepository
    * @param users List of users to access the resources, having only read access. These will be mapped to CDP roles using the RoleRepository
    * @param defaultUsersOwners List of default users to act as owners of the resources. These typically include `admin` and the services like hue or impala
    * @param zoneName Ranger Security Zone name to which the policy will be associated
    * @return Either a [[RangerPolicyGatewayError]] if there was an error upserting the policy,
    *         or the list of upserted [[PolicyAttachment]] for database, table and url.
    */
  def attachPolicy(
      database: String,
      table: String,
      url: String,
      owners: Seq[String],
      users: Seq[String],
      defaultUsersOwners: Seq[String],
      zoneName: String
  ): Either[RangerPolicyGatewayError, Seq[PolicyAttachment]] =
    for {
      owns <- rolesGroup(owners).leftMap(e => AttachRangerPolicyErr(e))
      usrs <- rolesGroup(users).leftMap(e => AttachRangerPolicyErr(e))
      cdpOwns = owns.flatMap(_.map(_.cdpRole))
      cdpUsrs = usrs.flatMap(_.map(_.cdpRole))
      dbPolicy = RangerPolicyGenerator.impalaDb(
        database,
        cdpOwns,
        cdpUsrs,
        defaultUsersOwners,
        zoneName)
      tblPolicy = RangerPolicyGenerator.impalaTable(
        database,
        table,
        cdpOwns,
        cdpUsrs,
        defaultUsersOwners,
        zoneName)
      urlPolicy = RangerPolicyGenerator.impalaUrl(
        database,
        table,
        url,
        cdpOwns,
        cdpUsrs,
        defaultUsersOwners,
        zoneName)
      dbPl  <- upsertPolicy(dbPolicy, Some(zoneName))
      tblPl <- upsertPolicy(tblPolicy, Some(zoneName))
      urlPl <- upsertPolicy(urlPolicy, Some(zoneName))
    } yield Seq(dbPl, tblPl, urlPl)

  /** Deletes the policies that allowed access to a specific table and url to a set of owners and users.
    *
    * @param database           Database name. It is used to define the table and url policy names to be deleted
    * @param table              Table name. It is used to define the table policy name and the url policy name to be deleted
    * @param url                Location of the data of the policy to be deleted
    * @param owners             List of users to act as owners of the resources to revoke access. These will be mapped to CDP roles using the RoleRepository
    * @param users              List of users to access the resources to revoke access. These will be mapped to CDP roles using the RoleRepository
    * @param defaultUsersOwners List of default users to act as owners of the resources to revoke access. These typically include `admin` and the services like hue or impala
    * @param zoneName           Ranger Security Zone name to which the policy to be deleted is associated
    * @return Either a [[RangerPolicyGatewayError]] if there was an error deleting the policy,
    *         or the list of deleted [[PolicyAttachment]] for table and url.
    *         If a policy doesn't exist, the method still returns a Right but the policy won't be included in the Right result.
    */
  def detachPolicy(
      database: String,
      table: String,
      url: String,
      owners: Seq[String],
      users: Seq[String],
      defaultUsersOwners: Seq[String],
      zoneName: String
  ): Either[RangerPolicyGatewayError, Seq[PolicyAttachment]] =
    for {
      owns <- rolesGroup(owners).leftMap(e => AttachRangerPolicyErr(e))
      usrs <- rolesGroup(users).leftMap(e => AttachRangerPolicyErr(e))
      cdpOwns = owns.flatMap(_.map(_.cdpRole))
      cdpUsrs = usrs.flatMap(_.map(_.cdpRole))
      tblPolicy = RangerPolicyGenerator.impalaTable(
        database,
        table,
        cdpOwns,
        cdpUsrs,
        defaultUsersOwners,
        zoneName)
      urlPolicy = RangerPolicyGenerator.impalaUrl(
        database,
        table,
        url,
        cdpOwns,
        cdpUsrs,
        defaultUsersOwners,
        zoneName)
      tblPl <- removePolicy(tblPolicy, Some(zoneName))
      urlPl <- removePolicy(urlPolicy, Some(zoneName))
    } yield tblPl.map(Seq(_)).getOrElse(Seq.empty[PolicyAttachment]) ++:
      urlPl.map(Seq(_)).getOrElse(Seq.empty[PolicyAttachment])

  private def upsertPolicy(
      policy: RangerPolicy,
      zoneName: Option[String]
  ): Either[UpsertPolicyErr, PolicyAttachment] =
    for {
      optP <- rangerClient
        .findPolicyByName(policy.service, policy.name, getSafeZoneName(zoneName))
        .leftMap(e => UpsertPolicyErr(e))
      pl <- optP
        .fold(rangerClient.createPolicy(policy))(p =>
          rangerClient.updatePolicy(policy.copy(id = p.id)))
        .leftMap(e => UpsertPolicyErr(e))
    } yield PolicyAttachment(s"${pl.id}", pl.name)

  private def removePolicy(
      policy: RangerPolicy,
      zoneName: Option[String]
  ): Either[UpsertPolicyErr, Option[PolicyAttachment]] =
    for {
      optP <- rangerClient
        .findPolicyByName(policy.service, policy.name, getSafeZoneName(zoneName))
        .leftMap(e => UpsertPolicyErr(e))
      pl <- optP
        .map(p =>
          rangerClient
            .deletePolicy(policy.copy(id = p.id))
            .map(_ => PolicyAttachment(s"${p.id}", policy.name))
            .leftMap(e => UpsertPolicyErr(e)))
        .sequence
    } yield pl

  private def rolesGroup(roles: Seq[String]): Either[RepositoryError, Seq[Option[Role]]] =
    roles.traverse(roleRepository.findById)

  private def getSafeZoneName(z: Option[String]): Option[String] = z match {
    case Some("") => None
    case s        => s
  }

}

object RangerPolicyGateway {
  def ranger(
      roleTable: String,
      roleTablePrimaryKey: String,
      rangerClient: RangerClient
  ): Either[RangerPolicyGatewayError, RangerPolicyGateway] = for {
    repo <- Repository
      .dynamoDB(roleTable, roleTablePrimaryKey, None)
      .leftMap(e => RangerPolicyGatewayInitErr(e))
  } yield new RangerPolicyGateway(new RoleDynamoDBRepository(repo), rangerClient)

  def rangerWithAudit(
      roleTable: String,
      roleTablePrimaryKey: String,
      rangerClient: RangerClient
  ): Either[RangerPolicyGatewayError, RangerPolicyGateway] = for {
    repo <- Repository
      .dynamoDBWithAudit(roleTable, roleTablePrimaryKey, None)
      .leftMap(e => RangerPolicyGatewayInitErr(e))
  } yield new RangerPolicyGateway(new RoleDynamoDBRepository(repo), rangerClient)

  /** Creates a RangerPolicyGateway using a ranger client and a repository based on Typesafe configuration files
    * @param rangerClient RangerClient to interface with Cloudera Ranger
    */
  def rangerWithConfig(rangerClient: RangerClient): RangerPolicyGateway =
    new RangerPolicyGateway(new RoleConfigRepository, rangerClient)
}
