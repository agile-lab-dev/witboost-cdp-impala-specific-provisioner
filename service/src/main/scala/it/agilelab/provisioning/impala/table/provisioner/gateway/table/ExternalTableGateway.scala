package it.agilelab.provisioning.impala.table.provisioner.gateway.table

import it.agilelab.provisioning.commons.audit.Audit
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider.ConnectionConfig
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.ddl.ImpalaDataDefinitionLanguageProvider
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.query.{
  SqlGateway,
  SqlGatewayError
}
import it.agilelab.provisioning.impala.table.provisioner.core.model.ExternalTable

trait ExternalTableGateway {

  /** Creates an external table, creating the associated database if it doesn't exist
    * @param connectionConfigurations Connection configuration to build the JDBC connection
    * @param externalTable External table information
    * @param ifNotExists If set to true, the method won't fail if the table already exists
    */
  def create(
      connectionConfigurations: ConnectionConfig,
      externalTable: ExternalTable,
      ifNotExists: Boolean
  ): Either[SqlGatewayError, Unit]

  /** Drops an external table
    * @param connectionConfigurations Connection configuration to build the JDBC connection
    * @param externalTable External table information
    * @param ifExists If set to true, the method won't fail if the table doesn't exist
    */
  def drop(
      connectionConfigurations: ConnectionConfig,
      externalTable: ExternalTable,
      ifExists: Boolean
  ): Either[SqlGatewayError, Unit]
}

object ExternalTableGateway {

  def impala(deployUser: String, deployPassword: String): ExternalTableGateway =
    new ImpalaExternalTableGateway(
      deployUser,
      deployPassword,
      new ImpalaDataDefinitionLanguageProvider(),
      SqlGateway.impala()
    )

  def impalaWithAudit(deployUser: String, deployPassword: String): ExternalTableGateway =
    new ImpalaExternalTableGatewayWithAudit(
      impala(deployUser, deployPassword),
      Audit.default("ImpalaExternalTableGateway"))

  def kerberizedImpala(): ExternalTableGateway = new ImpalaExternalTableGateway(
    "",
    "",
    new ImpalaDataDefinitionLanguageProvider(),
    SqlGateway.kerberizedImpala()
  )

  def kerberizedImpalaWithAudit(): ExternalTableGateway = new ImpalaExternalTableGatewayWithAudit(
    kerberizedImpala(),
    Audit.default("ImpalaExternalTableGateway"))
}
