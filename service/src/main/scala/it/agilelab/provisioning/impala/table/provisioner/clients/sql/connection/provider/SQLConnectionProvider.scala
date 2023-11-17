package it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider

import java.sql.{ Connection, DriverManager }

class SQLConnectionProvider(
    driver: String,
    sqlConnectionStringProvider: ConnectionStringProvider
) extends ConnectionProvider {

  override def get(connectionConfig: ConnectionConfig): Connection = {
    Class.forName(driver)
    DriverManager.getConnection(sqlConnectionStringProvider.get(connectionConfig))
  }

}
