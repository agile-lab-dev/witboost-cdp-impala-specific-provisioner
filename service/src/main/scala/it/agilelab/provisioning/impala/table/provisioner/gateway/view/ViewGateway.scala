package it.agilelab.provisioning.impala.table.provisioner.gateway.view

import it.agilelab.provisioning.commons.audit.Audit
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider.ConnectionConfig
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.ddl.ImpalaDataDefinitionLanguageProvider
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.query.{
  SqlGateway,
  SqlGatewayError
}
import it.agilelab.provisioning.impala.table.provisioner.core.model.{
  ExternalTable,
  ImpalaEntityResource,
  ImpalaView
}
import it.agilelab.provisioning.impala.table.provisioner.gateway.table.{
  ImpalaExternalTableGateway,
  ImpalaExternalTableGatewayWithAudit
}

trait ViewGateway {

  /** Creates a view. The database and table from which it reads must exist.
    * @param connectionConfigurations Connection configuration to build the JDBC connection
    * @param impalaView View information
    * @param ifNotExists If set to true, the method won't fail if the view already exists
    */
  def create(
      connectionConfigurations: ConnectionConfig,
      impalaView: ImpalaView,
      ifNotExists: Boolean
  ): Either[SqlGatewayError, ImpalaEntityResource]

  /** Drops a view
    * @param connectionConfigurations Connection configuration to build the JDBC connection
    * @param impalaView View information
    * @param ifExists If set to true, the method won't fail if the view doesn't exist
    */
  def drop(
      connectionConfigurations: ConnectionConfig,
      impalaView: ImpalaView,
      ifExists: Boolean
  ): Either[SqlGatewayError, ImpalaEntityResource]
}

object ViewGateway {

  def impala(deployUser: String, deployPassword: String): ViewGateway =
    new ImpalaViewGateway(
      deployUser,
      deployPassword,
      new ImpalaDataDefinitionLanguageProvider(),
      SqlGateway.impala()
    )

  def impalaWithAudit(deployUser: String, deployPassword: String): ViewGateway =
    new ImpalaViewGatewayWithAudit(
      impala(deployUser, deployPassword),
      Audit.default("ImpalaViewGateway"))

  def kerberizedImpala(): ViewGateway = new ImpalaViewGateway(
    "",
    "",
    new ImpalaDataDefinitionLanguageProvider(),
    SqlGateway.kerberizedImpala()
  )

  def kerberizedImpalaWithAudit(): ViewGateway =
    new ImpalaViewGatewayWithAudit(kerberizedImpala(), Audit.default("ImpalaViewGateway"))
}
