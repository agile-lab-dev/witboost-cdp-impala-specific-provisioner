package it.agilelab.provisioning.impala.table.provisioner.gateway.mapper

import cats.implicits._
import it.agilelab.provisioning.impala.table.provisioner.core.model.{
  ExternalTable,
  Field,
  ImpalaCdw
}
import it.agilelab.provisioning.impala.table.provisioner.core.support.ImpalaDataTypeSupport
import it.agilelab.provisioning.mesh.self.service.api.model.openmetadata.Column
import it.agilelab.provisioning.mesh.self.service.core.gateway.ComponentGatewayError

object ExternalTableMapper extends ImpalaDataTypeSupport {
  def map(
      schema: Seq[Column],
      impalaSpecific: ImpalaCdw
  ): Either[ComponentGatewayError, ExternalTable] = for {
    tableSchema <- schema
      .filter(c => impalaSpecific.partitions.forall(seq => !seq.contains(c.name)))
      .map(toField)
      .sequence
    partitions <- schema
      .filter(c => impalaSpecific.partitions.exists(seq => seq.contains(c.name)))
      .map(toField)
      .sequence
  } yield ExternalTable(
    database = impalaSpecific.databaseName,
    tableName = impalaSpecific.tableName,
    schema = tableSchema,
    partitions = partitions,
    location = impalaSpecific.location,
    format = impalaSpecific.format
  )

  private def toField(c: Column): Either[ComponentGatewayError, Field] = for {
    dt <- fromOpenMetadataColumn(c).leftMap(s => ComponentGatewayError(s))
  } yield Field(c.name, dt, c.description)
}
