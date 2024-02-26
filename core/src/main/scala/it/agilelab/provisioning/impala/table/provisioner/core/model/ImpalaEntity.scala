package it.agilelab.provisioning.impala.table.provisioner.core.model

sealed trait ImpalaEntity {
  def database: String
  def name: String
  def schema: Seq[Field]
}

final case class ExternalTable(
    override val database: String,
    override val name: String,
    override val schema: Seq[Field],
    partitions: Seq[Field],
    location: String,
    format: ImpalaFormat
) extends ImpalaEntity

final case class ImpalaView(
    override val database: String,
    override val name: String,
    override val schema: Seq[Field],
    readsFromTableName: String
) extends ImpalaEntity
