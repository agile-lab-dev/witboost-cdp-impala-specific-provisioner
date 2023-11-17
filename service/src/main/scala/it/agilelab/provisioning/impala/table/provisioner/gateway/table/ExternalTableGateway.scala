package it.agilelab.provisioning.impala.table.provisioner.gateway.table

import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.pattern.ConnectionStringPatterns
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider.{
  ConnectionConfig,
  ConnectionStringProvider,
  SQLConnectionProvider
}
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.ddl.ImpalaDataDefinitionLanguageProvider
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.driver.Drivers
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.query.{
  DefaultSQLGateway,
  SqlGatewayError
}
import it.agilelab.provisioning.impala.table.provisioner.core.model.ExternalTable

trait ExternalTableGateway {

  def create(
      connectionConfigurations: ConnectionConfig,
      externalTable: ExternalTable,
      ifNotExists: Boolean
  ): Either[SqlGatewayError, Unit]
}

object ExternalTableGateway {

  def impala(deployUser: String, deployPassword: String): ExternalTableGateway =
    new ImpalaExternalTableGateway(
      deployUser,
      deployPassword,
      new ImpalaDataDefinitionLanguageProvider(),
      new DefaultSQLGateway(
        new SQLConnectionProvider(
          Drivers.impala,
          new ConnectionStringProvider(ConnectionStringPatterns.impala)
        )
      )
    )
}
