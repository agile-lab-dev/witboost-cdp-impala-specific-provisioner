package it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.pattern

object ConnectionStringPatterns {
  val impala =
    "jdbc:impala://%s:%s/%s;AuthMech=3;transportMode=http;httpPath=cliservice;ssl=1;UID=%s;PWD=%s"
}
