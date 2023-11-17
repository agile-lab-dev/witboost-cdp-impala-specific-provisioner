package it.agilelab.provisioning.impala.table.provisioner.core.model

final case class Acl(
    owners: Seq[String],
    users: Seq[String]
)
