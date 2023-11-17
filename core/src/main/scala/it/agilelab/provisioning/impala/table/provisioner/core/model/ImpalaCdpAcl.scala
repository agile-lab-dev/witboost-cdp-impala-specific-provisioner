package it.agilelab.provisioning.impala.table.provisioner.core.model

final case class ImpalaCdpAcl(
    attachedPolicies: Seq[PolicyAttachment],
    detachedPolicies: Seq[PolicyAttachment]
)
