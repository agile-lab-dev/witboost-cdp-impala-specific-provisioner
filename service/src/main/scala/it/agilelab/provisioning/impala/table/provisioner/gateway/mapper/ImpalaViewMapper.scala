package it.agilelab.provisioning.impala.table.provisioner.gateway.mapper

import cats.implicits.{ toBifunctorOps, toTraverseOps }
import it.agilelab.provisioning.impala.table.provisioner.core.model.{
  Field,
  ImpalaView,
  ImpalaViewCdw
}
import it.agilelab.provisioning.impala.table.provisioner.core.support.ImpalaDataTypeSupport
import it.agilelab.provisioning.mesh.self.service.api.model.openmetadata.Column
import it.agilelab.provisioning.mesh.self.service.core.gateway.ComponentGatewayError

object ImpalaViewMapper extends ImpalaDataTypeSupport {
  def map(
      schema: Seq[Column],
      impalaSpecific: ImpalaViewCdw
  ): Either[ComponentGatewayError, ImpalaView] =
    schema
      .map(toField)
      .sequence
      .map(viewSchema =>
        ImpalaView(
          database = impalaSpecific.databaseName,
          name = impalaSpecific.viewName,
          schema = viewSchema,
          readsFromTableName = impalaSpecific.tableName
        ))

  private def toField(c: Column): Either[ComponentGatewayError, Field] = for {
    dt <- fromOpenMetadataColumn(c).leftMap(s => ComponentGatewayError(s))
  } yield Field(c.name, dt, c.description)
}
