package it.agilelab.provisioning.impala.table.provisioner.gateway.table

import cats.implicits.showInterpolator
import it.agilelab.provisioning.commons.audit.Audit
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider.ConnectionConfig
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.query.SqlGatewayError
import it.agilelab.provisioning.impala.table.provisioner.core.model.ExternalTable

class ImpalaExternalTableGatewayWithAudit(
    impalaExternalTableGateway: ExternalTableGateway,
    audit: Audit
) extends ExternalTableGateway {

  private val INFO_MSG = "Executing %s"

  override def create(
      connectionConfigurations: ConnectionConfig,
      externalTable: ExternalTable,
      ifNotExists: Boolean
  ): Either[SqlGatewayError, Unit] = {
    val action = s"CreateExternalTable($externalTable)"
    audit.info(INFO_MSG.format(action))
    val result =
      impalaExternalTableGateway.create(connectionConfigurations, externalTable, ifNotExists)
    auditWithinResult(result, action)
    result
  }

  override def drop(
      connectionConfigurations: ConnectionConfig,
      externalTable: ExternalTable,
      ifExists: Boolean
  ): Either[SqlGatewayError, Unit] = {
    val action = s"DropExternalTable($externalTable)"
    audit.info(INFO_MSG.format(action))
    val result =
      impalaExternalTableGateway.drop(connectionConfigurations, externalTable, ifExists)
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
