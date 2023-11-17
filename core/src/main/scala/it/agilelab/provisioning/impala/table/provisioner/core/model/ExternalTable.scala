package it.agilelab.provisioning.impala.table.provisioner.core.model

final case class ExternalTable(
    database: String,
    tableName: String,
    schema: Seq[Field],
    partitions: Seq[Field],
    location: String,
    format: ImpalaFormat
)
