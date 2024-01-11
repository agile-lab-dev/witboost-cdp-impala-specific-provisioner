package it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.role

import it.agilelab.provisioning.commons.client.ranger.model.{ RangerRole, RoleMember }

object RangerRoleGenerator {

  private val OwnerRoleNamePattern = "%s_owner"
  private val UserRoleNamePattern = "%s_read"

  def generateOwnerRoleName(rolePrefix: String): String =
    OwnerRoleNamePattern.format(clean(rolePrefix))

  def generateUserRoleName(rolePrefix: String): String =
    UserRoleNamePattern.format(clean(rolePrefix))

  def clean(string: String): String = string.replaceAll("[^A-Za-z0-9_]", "_")

  def role(
      roleName: String,
      users: Seq[String],
      ownerUsers: Seq[String],
      groups: Seq[String],
      ownerGroups: Seq[String],
      roles: Seq[String]
  ): RangerRole = empty(clean(roleName))
    .copy(
      groups = groups.map(g => RoleMember(g, isAdmin = false)) ++
        ownerGroups.map(g => RoleMember(g, isAdmin = true)),
      users = users.map(u => RoleMember(u, isAdmin = false)) ++
        ownerUsers.map(u => RoleMember(u, isAdmin = true)),
      roles = roles.map(r => RoleMember(r, isAdmin = false))
    )

  def empty(roleName: String): RangerRole = new RangerRole(
    id = 0,
    isEnabled = true,
    name = roleName,
    description = "",
    groups = Seq.empty,
    users = Seq.empty,
    roles = Seq.empty)

}
