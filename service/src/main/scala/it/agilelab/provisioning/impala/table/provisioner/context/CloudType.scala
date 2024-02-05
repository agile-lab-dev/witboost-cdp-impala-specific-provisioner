package it.agilelab.provisioning.impala.table.provisioner.context

object CloudType {
  type CloudType = CloudType.Value
  object CloudType extends Enumeration {
    val Public: CloudType.Value = Value("public")
    val Private: CloudType.Value = Value("private")
  }
}
