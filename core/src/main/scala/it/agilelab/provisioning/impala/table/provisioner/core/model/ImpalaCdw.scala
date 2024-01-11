package it.agilelab.provisioning.impala.table.provisioner.core.model

final case class ImpalaCdw(
    databaseName: String,
    tableName: String,
    cdpEnvironment: String,
    cdwVirtualWarehouse: String,
    format: ImpalaFormat,
    location: String,
    partitions: Option[Seq[String]]
)
