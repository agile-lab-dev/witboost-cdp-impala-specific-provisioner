package it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.policy

import cats.implicits._
import it.agilelab.provisioning.commons.client.ranger.RangerClient
import it.agilelab.provisioning.commons.client.ranger.model.RangerPolicy
import it.agilelab.provisioning.impala.table.provisioner.core.model.PolicyAttachment
import it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.policy.RangerPolicyGatewayError.{
  DeletePolicyErr,
  UpsertPolicyErr
}

class RangerPolicyGateway(
    rangerClient: RangerClient
) {

  /** Upserts a set of policies to allow access to a specific database, table and url to a set of owners and users.
    * If the policy already exists for the database and/or table, it is updated with the received parameters.
    * Otherwise, a new one is created.
    * @param database Database name. It is used to define the database, table, and url policy name
    * @param table Table name. It is used to define the table policy name and the url policy name
    * @param url Location of the data
    * @param ownerRoleName Role name to act as owners of the resources, having read/write access.
    * @param userRoleName Role name to access the resources, having only read access.
    * @param defaultUsersOwners List of default users to act as owners of the resources. These typically include `admin` and the services like hue or impala
    * @param zoneName Ranger Security Zone name to which the policy will be associated
    * @return Either a [[RangerPolicyGatewayError]] if there was an error upserting the policy,
    *         or the list of upserted [[PolicyAttachment]] for database, table and url.
    */
  def upsertPolicies(
      database: String,
      table: String,
      url: String,
      ownerRoleName: String,
      userRoleName: Option[String],
      defaultUsersOwners: Seq[String],
      zoneName: String
  ): Either[RangerPolicyGatewayError, Seq[PolicyAttachment]] = {
    val dbPolicy = RangerPolicyGenerator.impalaDb(
      database,
      ownerRoleName,
      userRoleName,
      defaultUsersOwners,
      zoneName)
    val tblPolicy = RangerPolicyGenerator.impalaTable(
      database,
      table,
      ownerRoleName,
      userRoleName,
      defaultUsersOwners,
      zoneName)
    for {
      dbPl  <- upsertPolicy(dbPolicy, Some(zoneName))
      tblPl <- upsertPolicy(tblPolicy, Some(zoneName))
    } yield Seq(dbPl, tblPl)
  }

  /** Deletes the policies that allowed access to a specific table to a set of owners and users.
    * Updates policies at data product level removing the user role (if passed) from them.
    *
    * @param database           Database name. It is used to define the table policy names to be deleted
    * @param table              Table name. It is used to define the table policy name and the url policy name to be deleted
    * @param url                Location of the data of the policy to be deleted
    * @param userRoleName       Role name to access the resources, having only read access.
    * @param zoneName           Ranger Security Zone name to which the policy to be deleted is associated
    * @return Either a [[RangerPolicyGatewayError]] if there was an error deleting the policy,
    *         or the list of deleted [[PolicyAttachment]] for table.
    *         If a policy doesn't exist, the method still returns a Right but the policy won't be included in the Right result.
    */
  def deletePolicies(
      database: String,
      table: String,
      url: String,
      userRoleName: Option[String],
      zoneName: String
  ): Either[RangerPolicyGatewayError, Seq[PolicyAttachment]] = {
    val dbPolicy = RangerPolicyGenerator.impalaDb(database, "", userRoleName, Seq.empty, zoneName)
    val tablePolicy =
      RangerPolicyGenerator.impalaTable(database, table, "", userRoleName, Seq.empty, zoneName)
    for {
      optP <- rangerClient
        .findPolicyByName(dbPolicy.service, dbPolicy.name, getSafeZoneName(Some(zoneName)))
        .leftMap(e => UpsertPolicyErr(e))
      _ <- optP
        .flatMap { registeredPolicy =>
          userRoleName.map { roleName =>
            rangerClient
              .updatePolicy(RangerPolicyGenerator.policyWithRemovedRole(registeredPolicy, roleName))
              .leftMap(e => UpsertPolicyErr(e))
          }
        }
        .getOrElse(Right(()))
      tblPl <- removePolicy(tablePolicy, Some(zoneName))
    } yield tblPl.map(Seq(_)).getOrElse(Seq.empty[PolicyAttachment])
  }

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
          rangerClient.updatePolicy(
            RangerPolicyGenerator
              .policyWithMergedPolicyItems(policy.copy(id = p.id), p.policyItems)))
        .leftMap(e => UpsertPolicyErr(e))
    } yield PolicyAttachment(s"${pl.id}", pl.name)

  private def removePolicy(
      policy: RangerPolicy,
      zoneName: Option[String]
  ): Either[DeletePolicyErr, Option[PolicyAttachment]] =
    for {
      optP <- rangerClient
        .findPolicyByName(policy.service, policy.name, getSafeZoneName(zoneName))
        .leftMap(e => DeletePolicyErr(e))
      pl <- optP
        .map(p =>
          rangerClient
            .deletePolicy(p)
            .map(_ => PolicyAttachment(s"${p.id}", policy.name))
            .leftMap(e => DeletePolicyErr(e)))
        .sequence
    } yield pl

  private def getSafeZoneName(z: Option[String]): Option[String] = z match {
    case Some("") => None
    case s        => s
  }

}

object RangerPolicyGateway {
  def ranger(rangerClient: RangerClient): RangerPolicyGateway =
    new RangerPolicyGateway(rangerClient)
}
