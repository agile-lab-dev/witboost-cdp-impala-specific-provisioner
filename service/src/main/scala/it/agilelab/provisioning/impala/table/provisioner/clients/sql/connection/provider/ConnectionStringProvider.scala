package it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider

import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider.ConnectionProviderError.ParseConnectionStringErr

abstract class ConnectionStringProvider(pattern: String) {
  def get(connectionConfig: ConnectionConfig): Either[ConnectionProviderError, String]
}

class UsernamePasswordConnectionStringProvider(pattern: String)
    extends ConnectionStringProvider(pattern) {

  override def get(connectionConfig: ConnectionConfig): Either[ConnectionProviderError, String] =
    connectionConfig match {
      case UsernamePasswordConnectionConfig(host, port, schema, user, password, useSSL) =>
        val ssl: Int = if (useSSL) 1 else 0
        Right(pattern.format(host, port, schema, ssl, user, password))
      case _ => Left(ParseConnectionStringErr(connectionConfig, pattern))
    }
}

class KerberizedConnectionStringProvider(pattern: String)
    extends ConnectionStringProvider(pattern) {
  override def get(connectionConfig: ConnectionConfig): Either[ConnectionProviderError, String] =
    connectionConfig match {
      case KerberosConnectionConfig(
            host,
            port,
            schema,
            krbRealm,
            krbHostFQDN,
            krbServiceName,
            useSSL) =>
        val ssl: Int = if (useSSL) 1 else 0
        Right(
          pattern.format(
            host,
            port,
            schema,
            krbRealm.getOrElse(""),
            krbHostFQDN,
            krbServiceName,
            ssl)
        )
      case _ => Left(ParseConnectionStringErr(connectionConfig, pattern))
    }
}
