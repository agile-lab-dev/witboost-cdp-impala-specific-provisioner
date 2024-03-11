package it.agilelab.provisioning.impala.table.provisioner.gateway.mapper

import cats.implicits.{ toBifunctorOps, toTraverseOps }
import it.agilelab.provisioning.impala.table.provisioner.core.model.{
  Field,
  ImpalaView,
  ImpalaViewCdw,
  PrivateImpalaStorageAreaViewCdw,
  PrivateImpalaViewCdw
}
import it.agilelab.provisioning.impala.table.provisioner.core.support.ImpalaDataTypeSupport
import it.agilelab.provisioning.mesh.self.service.api.model.openmetadata.Column
import it.agilelab.provisioning.mesh.self.service.core.gateway.ComponentGatewayError

object ImpalaViewMapper extends ImpalaDataTypeSupport {

  /** Validates an API object for an Impala view
    * For Simple [[PrivateImpalaViewCdw]] it checks that the accompanying schema is non-empty
    * For View Storage Areas [[PrivateImpalaStorageAreaViewCdw]] it checks that the query statement is non-empty
    *
    * @param schema Seq of OpenMetadata columns
    * @param impalaViewCdw ImpalaViewCdw with the information
    * @return None if view is valid, Some with an error string if the view is not valid
    */
  def isValidView(
      schema: Seq[Column],
      impalaViewCdw: ImpalaViewCdw
  ): Option[String] =
    impalaViewCdw match {
      case _: PrivateImpalaViewCdw =>
        Option.when(schema.isEmpty)(
          "Cannot create an Impala view that doesn't define neither a schema nor a query source statement")
      case sp: PrivateImpalaStorageAreaViewCdw =>
        Option.when(sp.queryStatement.isEmpty)(
          "Cannot create an Impala view with an empty query source statement"
        )
    }

  def map(
      schema: Seq[Column],
      impalaSpecific: ImpalaViewCdw
  ): Either[ComponentGatewayError, ImpalaView] =
    isValidView(schema, impalaSpecific) match {
      case Some(error) => Left(ComponentGatewayError(error))
      case None =>
        schema
          .map(toField)
          .sequence
          .map(viewSchema =>
            impalaSpecific match {
              case PrivateImpalaViewCdw(databaseName, tableName, viewName) =>
                ImpalaView(
                  database = databaseName,
                  name = viewName,
                  schema = viewSchema,
                  readsFromSourceName = Some(tableName),
                  querySourceStatement = None
                )
              case PrivateImpalaStorageAreaViewCdw(databaseName, viewName, queryStatement, _) =>
                ImpalaView(
                  database = databaseName,
                  name = viewName,
                  schema = viewSchema,
                  readsFromSourceName = None,
                  querySourceStatement = Some(queryStatement)
                )
            })
    }

  private def toField(c: Column): Either[ComponentGatewayError, Field] = for {
    dt <- fromOpenMetadataColumn(c).leftMap(s => ComponentGatewayError(s))
  } yield Field(c.name, dt, c.description)
}
