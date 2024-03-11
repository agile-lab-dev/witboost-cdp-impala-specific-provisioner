package it.agilelab.provisioning.impala.table.provisioner.gateway.view

import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider.ConnectionConfig
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.ddl.ImpalaDataDefinitionLanguageProvider
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.query.{
  SqlGateway,
  SqlGatewayError
}
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaView

class ImpalaViewGateway(
    deployUser: String,
    deployPassword: String,
    ddlProvider: ImpalaDataDefinitionLanguageProvider,
    sqlQueryExecutor: SqlGateway
) extends ViewGateway {

  override def create(
      connectionConfigurations: ConnectionConfig,
      impalaView: ImpalaView,
      ifNotExists: Boolean
  ): Either[SqlGatewayError, Unit] =
    // TODO - Validate again the query if existent before executing it
    sqlQueryExecutor
      .executeDDL(
        connectionConfigurations.setCredentials(user = deployUser, password = deployPassword),
        ddlProvider.createView(impalaView, ifNotExists)
      )
      .map(_ => ())

  override def drop(
      connectionConfigurations: ConnectionConfig,
      impalaView: ImpalaView,
      ifExists: Boolean
  ): Either[SqlGatewayError, Unit] =
    sqlQueryExecutor
      .executeDDL(
        connectionConfigurations.setCredentials(user = deployUser, password = deployPassword),
        ddlProvider.dropView(impalaView, ifExists))
      .map(_ => ())
}
