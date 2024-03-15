package it.agilelab.provisioning.impala.table.provisioner.gateway.view

import cats.implicits.showInterpolator
import it.agilelab.provisioning.commons.audit.Audit
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider.ConnectionConfig
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.query.SqlGatewayError
import it.agilelab.provisioning.impala.table.provisioner.core.model.{
  ImpalaEntityResource,
  ImpalaView
}

class ImpalaViewGatewayWithAudit(
    impalaViewGateway: ViewGateway,
    audit: Audit
) extends ViewGateway {

  private val INFO_MSG = "Executing %s"

  override def create(
      connectionConfigurations: ConnectionConfig,
      impalaView: ImpalaView,
      ifNotExists: Boolean
  ): Either[SqlGatewayError, ImpalaEntityResource] = {
    val action = s"CreateImpalaView($impalaView)"
    audit.info(INFO_MSG.format(action))
    val result =
      impalaViewGateway.create(connectionConfigurations, impalaView, ifNotExists)
    auditWithinResult(result, action)
    result
  }

  override def drop(
      connectionConfigurations: ConnectionConfig,
      impalaView: ImpalaView,
      ifExists: Boolean
  ): Either[SqlGatewayError, ImpalaEntityResource] = {
    val action = s"DropImpalaView($impalaView)"
    audit.info(INFO_MSG.format(action))
    val result =
      impalaViewGateway.drop(connectionConfigurations, impalaView, ifExists)
    auditWithinResult(result, action)
    result
  }

  private def auditWithinResult[D](
      result: Either[SqlGatewayError, D],
      action: String
  ): Unit =
    result match {
      case Right(_) => audit.info(show"$action completed successfully")
      case Left(l)  => audit.error(show"$action failed. Details: $l")
    }
}
