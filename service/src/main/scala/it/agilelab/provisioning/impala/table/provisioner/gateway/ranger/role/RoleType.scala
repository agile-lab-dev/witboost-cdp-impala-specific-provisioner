package it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.role

sealed trait RoleType

object OwnerRoleType extends RoleType
object UserRoleType extends RoleType
