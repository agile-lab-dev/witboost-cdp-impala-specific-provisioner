package it.agilelab.provisioning.impala.table.provisioner.gateway.mapper

import it.agilelab.provisioning.mesh.self.service.api.model.Component.OutputPort
import it.agilelab.provisioning.mesh.self.service.api.model.openmetadata.Column
import it.agilelab.provisioning.mesh.self.service.core.gateway.ComponentGatewayError
import cats.implicits._
import it.agilelab.provisioning.impala.table.provisioner.core.model.{
  ExternalTable,
  Field,
  ImpalaCdw
}
import it.agilelab.provisioning.impala.table.provisioner.core.support.ImpalaDataTypeSupport

object ExternalTableMapper extends ImpalaDataTypeSupport {
  def map(
      component: OutputPort[ImpalaCdw]
  ): Either[ComponentGatewayError, ExternalTable] = for {
    schema <- component.dataContract.schema
      .filter(c => component.specific.partitions.forall(seq => !seq.contains(c.name)))
      .map(toField)
      .sequence
    partitions <- component.dataContract.schema
      .filter(c => component.specific.partitions.exists(seq => seq.contains(c.name)))
      .map(toField)
      .sequence
  } yield ExternalTable(
    database = component.specific.databaseName,
    tableName = component.specific.tableName,
    schema = schema,
    partitions = partitions,
    location = component.specific.location,
    format = component.specific.format
  )

  private def toField(c: Column): Either[ComponentGatewayError, Field] = for {
    dt <- fromOpenMetadataColumn(c).leftMap(s => ComponentGatewayError(s))
  } yield Field(c.name, dt, c.description)
}
