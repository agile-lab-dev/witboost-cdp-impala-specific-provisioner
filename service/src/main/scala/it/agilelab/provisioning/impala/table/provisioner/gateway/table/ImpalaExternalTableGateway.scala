package it.agilelab.provisioning.impala.table.provisioner.gateway.table

import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider.ConnectionConfig
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.ddl.ImpalaDataDefinitionLanguageProvider
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.query.{
  DefaultSQLGateway,
  SqlGateway,
  SqlGatewayError
}
import it.agilelab.provisioning.impala.table.provisioner.core.model.ExternalTable

class ImpalaExternalTableGateway(
    deployUser: String,
    deployPassword: String,
    ddlProvider: ImpalaDataDefinitionLanguageProvider,
    sqlQueryExecutor: SqlGateway
) extends ExternalTableGateway {

  override def create(
      connectionConfigurations: ConnectionConfig,
      externalTable: ExternalTable,
      ifNotExists: Boolean
  ): Either[SqlGatewayError, Unit] =
    sqlQueryExecutor
      .executeDDLs(
        connectionConfigurations.setCredentials(user = deployUser, password = deployPassword),
        Seq(
          ddlProvider.createDataBase(externalTable.database, ifNotExists = true),
          ddlProvider.createExternalTable(externalTable, ifNotExists)
        )
      )
      .map(_ => ())

  override def drop(
      connectionConfigurations: ConnectionConfig,
      externalTable: ExternalTable,
      ifExists: Boolean
  ): Either[SqlGatewayError, Unit] =
    sqlQueryExecutor
      .executeDDL(
        connectionConfigurations.setCredentials(user = deployUser, password = deployPassword),
        ddlProvider.dropExternalTable(externalTable, ifExists))
      .map(_ => ())
}
