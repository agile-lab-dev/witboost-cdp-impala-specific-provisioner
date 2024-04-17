package it.agilelab.provisioning.impala.table.provisioner.gateway.ranger

final case class RangerUserConfig(
    users: List[UserConfig],
    groups: List[UserConfig],
    addEntitiesToRole: Boolean,
    addEntitiesToSecurityZone: Boolean
)
final case class UserConfig(name: String, isAdmin: Boolean)
