package it.agilelab.provisioning.impala.table.provisioner.core.model

final case class ImpalaTableResource(
    table: ExternalTable,
    policies: ImpalaCdpAcl
)
