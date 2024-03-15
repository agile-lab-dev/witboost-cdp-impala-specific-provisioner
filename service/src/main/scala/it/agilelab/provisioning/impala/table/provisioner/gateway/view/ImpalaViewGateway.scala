package it.agilelab.provisioning.impala.table.provisioner.gateway.view

import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider.ConnectionConfig
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.ddl.ImpalaDataDefinitionLanguageProvider
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.query.{
  SqlGateway,
  SqlGatewayError
}
import it.agilelab.provisioning.impala.table.provisioner.core.model.{
  ImpalaEntityResource,
  ImpalaView
}

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
  ): Either[SqlGatewayError, ImpalaEntityResource] =
    for {
      // TODO - Validate again the query if existent before executing it
      _ <- sqlQueryExecutor
        .executeDDL(
          connectionConfigurations.setCredentials(user = deployUser, password = deployPassword),
          ddlProvider.createView(impalaView, ifNotExists)
        )
      jdbc <- sqlQueryExecutor.getConnectionString(connectionConfigurations)
    } yield ImpalaEntityResource(impalaView, jdbc)

  override def drop(
      connectionConfigurations: ConnectionConfig,
      impalaView: ImpalaView,
      ifExists: Boolean
  ): Either[SqlGatewayError, ImpalaEntityResource] =
    for {
      _ <- sqlQueryExecutor
        .executeDDL(
          connectionConfigurations.setCredentials(user = deployUser, password = deployPassword),
          ddlProvider.dropView(impalaView, ifExists))
      jdbc <- sqlQueryExecutor.getConnectionString(connectionConfigurations)
    } yield ImpalaEntityResource(impalaView, jdbc)
}
