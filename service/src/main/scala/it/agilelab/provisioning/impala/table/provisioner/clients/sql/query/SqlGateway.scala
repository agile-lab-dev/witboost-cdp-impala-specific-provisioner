package it.agilelab.provisioning.impala.table.provisioner.clients.sql.query

import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider.{
  ConnectionConfig,
  ConnectionProvider
}

trait SqlGateway {
  def executeDDL(connectionConfig: ConnectionConfig, ddl: String): Either[SqlGatewayError, Int]
  def executeDDLs(
      connectionConfig: ConnectionConfig,
      ddls: Seq[String]
  ): Either[SqlGatewayError, Int]

}

object SqlGateway {

  def impala(): SqlGateway =
    new DefaultSQLGateway(ConnectionProvider.impala())

  def kerberizedImpala(): SqlGateway =
    new DefaultSQLGateway(ConnectionProvider.kerberizedImpala())

}
