package it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.role

import cats.implicits.toBifunctorOps
import it.agilelab.provisioning.commons.client.ranger.RangerClient
import it.agilelab.provisioning.commons.client.ranger.model.RangerRole
import it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.role.RangerRoleGatewayError.{
  DeleteRoleErr,
  FindRoleErr,
  UpsertRoleErr
}

class RangerRoleGateway(val rangerClient: RangerClient) {

  /** Upserts the specified role.
    * If the role already exists, it is updated with the received parameters.
    * Otherwise, a new one is created.
    *
    * @param rolePrefix  The prefix to use for the role name
    * @param roleType    The role type
    * @param deployUser  The deploy user that will be admin of the role
    * @param ownerUsers  List of other users that will be admin of the role
    * @param ownerGroups List of group that will be admin of the role
    * @param users       List of the users in the role
    * @param groups      List of the groups in the role
    * @return Either a [[RangerRoleGatewayError]] if there was an error upserting the role,
    *         or the upserted [[RangerRole]].
    */
  def upsertRole(
      rolePrefix: String,
      roleType: RoleType,
      deployUser: String,
      ownerUsers: Seq[String],
      ownerGroups: Seq[String],
      users: Seq[String],
      groups: Seq[String]
  ): Either[RangerRoleGatewayError, RangerRole] = {
    val roleName = roleType match {
      case OwnerRoleType => RangerRoleGenerator.generateOwnerRoleName(rolePrefix)
      case UserRoleType  => RangerRoleGenerator.generateUserRoleName(rolePrefix)
    }
    upsert(
      RangerRoleGenerator.role(
        roleName = roleName,
        users = users,
        ownerUsers = List(deployUser) ++ ownerUsers,
        groups = groups,
        ownerGroups = ownerGroups,
        List.empty
      )
    )
  }

  /** Deletes the specified user role.
    * If the role doesn't exists, no error is thrown.
    *
    * @param userRolePrefix The prefix to use for the role name
    * @return Either a [[RangerRoleGatewayError]] if there was an error deleting the role,
    *         or nothing else.
    */
  def deleteUserRole(userRolePrefix: String): Either[RangerRoleGatewayError, Unit] =
    for {
      optUsrRole <- rangerClient
        .findRoleByName(RangerRoleGenerator.generateUserRoleName(userRolePrefix))
        .leftMap(e => FindRoleErr(e))
      deleted <- optUsrRole
        .map(r =>
          rangerClient
            .deleteRole(r)
            .leftMap(e => DeleteRoleErr(e)))
        .getOrElse(Right(()))
    } yield deleted

  private def upsert(
      role: RangerRole
  ): Either[RangerRoleGatewayError, RangerRole] =
    for {
      optR <- rangerClient
        .findRoleByName(role.name)
        .leftMap(e => FindRoleErr(e))
      rl <- optR
        .fold(rangerClient.createRole(role))(r =>
          rangerClient.updateRole(
            // Always preserve old admin principals when updating roles
            role.copy(
              id = r.id,
              users = (role.users ++ r.users.filter(_.isAdmin)).distinct,
              groups = (role.groups ++ r.groups.filter(_.isAdmin)).distinct,
              roles = (role.roles ++ r.roles.filter(_.isAdmin)).distinct
            )
          ))
        .leftMap(e => UpsertRoleErr(e))
    } yield rl
}

object RangerRoleGateway {
  def default(
      rangerClient: RangerClient
  ): RangerRoleGateway =
    new RangerRoleGateway(rangerClient)
}
