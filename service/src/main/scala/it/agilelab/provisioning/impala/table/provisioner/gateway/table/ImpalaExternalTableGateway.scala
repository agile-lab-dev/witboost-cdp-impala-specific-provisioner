package it.agilelab.provisioning.impala.table.provisioner.gateway.table

import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider.ConnectionConfig
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.ddl.ImpalaDataDefinitionLanguageProvider
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.query.{
  DefaultSQLGateway,
  SqlGateway,
  SqlGatewayError
}
import it.agilelab.provisioning.impala.table.provisioner.core.model.{
  ExternalTable,
  ImpalaEntityResource
}

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
  ): Either[SqlGatewayError, ImpalaEntityResource] =
    for {
      _ <- sqlQueryExecutor
        .executeDDLs(
          connectionConfigurations.setCredentials(user = deployUser, password = deployPassword),
          Seq(
            ddlProvider.createDataBase(externalTable.database, ifNotExists = true),
            ddlProvider.createExternalTable(externalTable, ifNotExists)
          )
        )
      jdbc <- sqlQueryExecutor.getConnectionString(
        // Used to avoid returning sensitive credentials data
        connectionConfigurations.setCredentials("<USER>", "<PASSWORD>"))
    } yield ImpalaEntityResource(externalTable, jdbc)

  override def refresh(
      connectionConfigurations: ConnectionConfig,
      externalTable: ExternalTable
  ): Either[SqlGatewayError, ImpalaEntityResource] =
    for {
      _ <- sqlQueryExecutor
        .executeDDLs(
          connectionConfigurations.setCredentials(user = deployUser, password = deployPassword),
          ddlProvider.refreshStatements(externalTable)
        )
      jdbc <- sqlQueryExecutor.getConnectionString(
        // Used to avoid returning sensitive credentials data
        connectionConfigurations.setCredentials("<USER>", "<PASSWORD>"))
    } yield ImpalaEntityResource(externalTable, jdbc)

  override def drop(
      connectionConfigurations: ConnectionConfig,
      externalTable: ExternalTable,
      ifExists: Boolean
  ): Either[SqlGatewayError, ImpalaEntityResource] =
    for {
      _ <- sqlQueryExecutor
        .executeDDL(
          connectionConfigurations.setCredentials(user = deployUser, password = deployPassword),
          ddlProvider.dropExternalTable(externalTable, ifExists))
      jdbc <- sqlQueryExecutor.getConnectionString(connectionConfigurations)
    } yield ImpalaEntityResource(externalTable, jdbc)

}
