package it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.pattern

object ConnectionStringPatterns {
  val impala =
    "jdbc:impala://%s:%s/%s;AuthMech=3;transportMode=http;httpPath=cliservice;ssl=%s;UID=%s;PWD=%s"
  val kerberizedImpala =
    "jdbc:impala://%s:%s/%s;AuthMech=1;KrbRealm=%s;KrbHostFQDN=%s;KrbAuthType=1;KrbServiceName=%s;ssl=%s"

}
