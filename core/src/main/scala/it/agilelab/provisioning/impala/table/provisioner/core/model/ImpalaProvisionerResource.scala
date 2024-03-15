package it.agilelab.provisioning.impala.table.provisioner.core.model

final case class ImpalaProvisionerResource(
    impalaEntityResource: ImpalaEntityResource,
    policies: ImpalaCdpAcl
)
