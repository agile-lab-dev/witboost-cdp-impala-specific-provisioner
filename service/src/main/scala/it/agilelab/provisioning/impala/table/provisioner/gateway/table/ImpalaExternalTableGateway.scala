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

  /** Creates an External Table using the received JDBC connection configuration
    * @param connectionConfigurations Connection configuration to build the JDBC connection string and connect to Impala.
    *                                 It <b>MUST NOT</b> contain sensitive data, as this information will be used to build the
    *                                 JDBC connection to be returned to the user as provision info. Sensitive data must be sent on the class constructor or be passed by configuration
    * @param externalTable External table information
    * @param ifNotExists If set to true, the method won't fail if the table already exists
    * @return
    */
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
