package it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection

import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider.ConnectionProviderError.ParseConnectionStringErr
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider.{
  KerberizedConnectionStringProvider,
  KerberosConnectionConfig,
  UsernamePasswordConnectionConfig,
  UsernamePasswordConnectionStringProvider
}
import org.scalatest.funsuite.AnyFunSuite

class ConnectionStringProviderTest extends AnyFunSuite {

  test("get username/password") {
    val userCC =
      UsernamePasswordConnectionConfig("hst1", "p22", "test", "usr", "pwd", useSSL = false)

    val actual =
      new UsernamePasswordConnectionStringProvider("jdbc:xx://%s:%s/%s;ssl=%s;USER=%s;PWD=%s").get(
        userCC)

    val expected = Right("jdbc:xx://hst1:p22/test;ssl=0;USER=usr;PWD=pwd")

    assert(actual == expected)
  }

  test("get username/password error if passing kerberos connection config") {
    val kCC = KerberosConnectionConfig(
      "hst1",
      "p22",
      "test",
      Some("realm"),
      "hostFQDN",
      "service",
      useSSL = false)
    val jdbcPattern = "jdbc:xx://%s:%s/%s;ssl=%s;USER=%s;PWD=%s"
    val actual = new UsernamePasswordConnectionStringProvider(jdbcPattern).get(kCC)

    val expected = Left(ParseConnectionStringErr(kCC, jdbcPattern))

    assert(actual == expected)
  }

  test("get kerberized with all fields as given") {
    val kCC = KerberosConnectionConfig(
      "hst1",
      "p22",
      "test",
      Some("realm"),
      "hostFQDN",
      "service",
      useSSL = true)

    val actual = new KerberizedConnectionStringProvider(
      "jdbc:xx://%s:%s/%s;KrbRealm=%s;KrbHostFQDN=%s;KrbServiceName=%s;ssl=%s").get(kCC)

    val expected = Right(
      "jdbc:xx://hst1:p22/test;KrbRealm=realm;KrbHostFQDN=hostFQDN;KrbServiceName=service;ssl=1")

    assert(actual == expected)
  }

  test("get kerberized with realm non given") {
    val kCC =
      KerberosConnectionConfig("hst1", "p22", "test", None, "hostFQDN", "service", useSSL = true)

    val actual = new KerberizedConnectionStringProvider(
      "jdbc:xx://%s:%s/%s;KrbRealm=%s;KrbHostFQDN=%s;KrbServiceName=%s;ssl=%s").get(kCC)

    val expected =
      Right("jdbc:xx://hst1:p22/test;KrbRealm=;KrbHostFQDN=hostFQDN;KrbServiceName=service;ssl=1")

    assert(actual == expected)
  }

  test("get kerberized error if passing user/password connection config") {
    val userCC =
      UsernamePasswordConnectionConfig("hst1", "p22", "test", "usr", "pwd", useSSL = false)
    val pattern = "jdbc:xx://%s:%s/%s;KrbRealm=%s;KrbHostFQDN=%s;KrbServiceName=%s;ssl=%s"
    val actual = new KerberizedConnectionStringProvider(pattern).get(userCC)
    val expected = Left(ParseConnectionStringErr(userCC, pattern))
    assert(actual == expected)
  }

}
