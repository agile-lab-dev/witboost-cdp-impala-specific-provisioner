package it.agilelab.provisioning.impala.table.provisioner.clients.sql.exception

final case class ConnectionException(cause: Throwable, message: String)
    extends Exception(message, cause)
