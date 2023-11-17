package it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider

class ConnectionStringProvider(pattern: String) {

  def get(connectionConfig: ConnectionConfig): String =
    pattern.format(
      connectionConfig.host,
      connectionConfig.port,
      connectionConfig.schema,
      connectionConfig.user,
      connectionConfig.password
    )

}
