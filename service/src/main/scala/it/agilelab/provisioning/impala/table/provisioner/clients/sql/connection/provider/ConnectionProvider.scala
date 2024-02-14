package it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider

import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.pattern.ConnectionStringPatterns
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.driver.Drivers

import java.sql.Connection

trait ConnectionProvider {

  def get(connectionConfig: ConnectionConfig): Either[ConnectionProviderError, Connection]

}

object ConnectionProvider {

  def impala(): ConnectionProvider =
    new SQLConnectionProvider(
      Drivers.impala,
      new UsernamePasswordConnectionStringProvider(ConnectionStringPatterns.impala)
    )

  def kerberizedImpala(): ConnectionProvider =
    new SQLConnectionProvider(
      Drivers.impala,
      new KerberizedConnectionStringProvider(ConnectionStringPatterns.kerberizedImpala)
    )
}
