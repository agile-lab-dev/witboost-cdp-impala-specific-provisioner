package it.agilelab.provisioning.impala.table.provisioner.clients.sql.query

import cats.implicits.showInterpolator
import it.agilelab.provisioning.commons.audit.Audit
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider.ConnectionConfig

class SqlGatewayWithAudit(sqlGateway: SqlGateway, audit: Audit) extends SqlGateway {

  private val INFO_MSG = "Executing statement %s"

  override def executeDDL(
      connectionConfig: ConnectionConfig,
      ddl: String
  ): Either[SqlGatewayError, Int] = {
    val action = s"ExecuteDDL(${ellipsize(ddl, 50)})"
    audit.info(INFO_MSG.format(action))
    val result = sqlGateway.executeDDL(connectionConfig, ddl)
    auditWithinResult(result, action)
    result
  }

  override def executeDDLs(
      connectionConfig: ConnectionConfig,
      ddls: Seq[String]
  ): Either[SqlGatewayError, Int] = {
    val action = s"ExecuteDDLS(${ddls.map(ellipsize(_, 50)).mkString("[", ",", "]")})"
    audit.info(INFO_MSG.format(action))
    val result = sqlGateway.executeDDLs(connectionConfig, ddls)
    auditWithinResult(result, action)
    result
  }

  private def ellipsize(s: String, limit: Int): String =
    if (s.length > limit) {
      s.substring(0, limit) + "..."
    } else s

  private def auditWithinResult[D](
      result: Either[SqlGatewayError, D],
      action: String
  ): Unit =
    result match {
      case Right(_) => audit.info(show"$action completed successfully")
      case Left(l)  => audit.error(show"$action failed. Details: $l")
    }
}
