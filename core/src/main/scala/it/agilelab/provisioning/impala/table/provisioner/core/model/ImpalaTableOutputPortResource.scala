package it.agilelab.provisioning.impala.table.provisioner.core.model

final case class ImpalaTableOutputPortResource(
    table: ExternalTable,
    policies: ImpalaCdpAcl
)
