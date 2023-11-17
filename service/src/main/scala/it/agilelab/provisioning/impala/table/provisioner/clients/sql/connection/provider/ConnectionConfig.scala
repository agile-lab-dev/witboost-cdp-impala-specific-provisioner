package it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider

final case class ConnectionConfig(
    host: String,
    port: String,
    schema: String,
    user: String,
    password: String
)
