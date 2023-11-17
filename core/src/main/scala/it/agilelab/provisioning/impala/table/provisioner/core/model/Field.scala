package it.agilelab.provisioning.impala.table.provisioner.core.model

final case class Field(name: String, `type`: ImpalaDataType, description: Option[String])
