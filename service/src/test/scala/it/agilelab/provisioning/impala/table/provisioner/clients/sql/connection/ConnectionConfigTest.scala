package it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection

import com.typesafe.config.ConfigFactory
import it.agilelab.provisioning.commons.config.ConfError.{ ConfDecodeErr, ConfKeyNotFoundErr }
import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.provider.{
  ConnectionConfig,
  KerberosConnectionConfig,
  UsernamePasswordConnectionConfig
}
import it.agilelab.provisioning.impala.table.provisioner.context.ApplicationConfiguration
import org.scalatest.funsuite.AnyFunSuite

class ConnectionConfigTest extends AnyFunSuite {

  test("UsernamePassword ConnectionConfig set credentials") {
    val cc = UsernamePasswordConnectionConfig(
      host = "host",
      port = "443",
      schema = "default",
      user = "",
      password = "",
      useSSL = true
    )

    val expected = UsernamePasswordConnectionConfig(
      host = "host",
      port = "443",
      schema = "default",
      user = "username",
      password = "pwd",
      useSSL = true
    )
    val actual = cc.setCredentials("username", "pwd")
    assert(actual == expected)
  }

  test("ssl rendering is correct on username password") {
    val cc = UsernamePasswordConnectionConfig(
      host = "host",
      port = "443",
      schema = "default",
      user = "",
      password = "",
      useSSL = true
    )
  }

  test("ssl rendering is correct on kerberos") {
    val cc = KerberosConnectionConfig(
      host = "host",
      port = "443",
      schema = "default",
      useSSL = true,
      krbHostFQDN = "host",
      krbServiceName = "service",
      krbRealm = None
    )
  }

  test("loading from config loads correctly a username/password config") {
    val config = ConfigFactory.parseString(
      """
        |    auth-type = "simple"
        |    port = 443
        |    schema = "default"
        |    ssl = true
        |""".stripMargin
    )

    val expected =
      Right(UsernamePasswordConnectionConfig("host", "443", "default", "", "", useSSL = true))
    val actual = ConnectionConfig.getFromConfig(config, "host")
    assert(actual == expected)
  }

  test("loading from config loads correctly a username/password config overriding values") {
    val config = ConfigFactory.parseString(
      """
        |    auth-type = "simple"
        |    port = 443
        |    schema = "default"
        |    ssl = true
        |""".stripMargin
    )

    val expected =
      Right(UsernamePasswordConnectionConfig("host", "12000", "myschema", "", "", useSSL = true))
    val actual = ConnectionConfig.getFromConfig(config, "host", Some(12000), Some("myschema"))
    assert(actual == expected)
  }

  test("loading from config fails on an incorrect username/password config") {
    val config = ConfigFactory.parseString(
      """
        |    auth-type = "simple"
        |    schema = "default"
        |    ssl = true
        |""".stripMargin
    )

    val expected = Left(ConfKeyNotFoundErr(ApplicationConfiguration.JDBC_PORT))
    val actual = ConnectionConfig.getFromConfig(config, "host")
    assert(actual == expected)
  }

  test("loading from config loads correctly a kerberos config") {
    val config = ConfigFactory.parseString(
      """
        |    auth-type = "kerberos"
        |    port = 21050
        |    schema = "default"
        |    KrbRealm = "CLUSTER.INTERNAL"
        |    KrbHostFQDN = "cluster-vm.cluster.internal"
        |    KrbServiceName = "impala"
        |    ssl = true
        |""".stripMargin
    )

    val expected =
      Right(
        KerberosConnectionConfig(
          host = "host",
          port = "21050",
          schema = "default",
          krbRealm = Some("CLUSTER.INTERNAL"),
          krbHostFQDN = "cluster-vm.cluster.internal",
          krbServiceName = "impala",
          useSSL = true
        ))
    val actual = ConnectionConfig.getFromConfig(config, "host")
    assert(actual == expected)
  }

  test("loading from config loads correctly a kerberos config with minimal required fields") {
    val config = ConfigFactory.parseString(
      """
        |    auth-type = "kerberos"
        |    port = 21050
        |    schema = "default"
        |    KrbServiceName = "impala"
        |    ssl = true
        |""".stripMargin
    )

    // If krbRealm is not set in the JDBC Connection String, the system's default one will be set
    // We expect that if no krbHostFQDN is set, is because is equal to the used host, as usually is the case
    val expected =
      Right(
        KerberosConnectionConfig(
          host = "host",
          port = "21050",
          schema = "default",
          krbRealm = None,
          krbHostFQDN = "host",
          krbServiceName = "impala",
          useSSL = true
        ))
    val actual = ConnectionConfig.getFromConfig(config, "host")
    assert(actual == expected)
  }

  test("loading from config fails on an incorrect kerberos config") {
    val config = ConfigFactory.parseString(
      """
        |    auth-type = "kerberos"
        |    port = 21050
        |    schema = "default"
        |    ssl = true
        |""".stripMargin
    )

    val expected = Left(ConfKeyNotFoundErr(ApplicationConfiguration.JDBC_KRBSERVICENAME))
    val actual = ConnectionConfig.getFromConfig(config, "host")
    assert(actual == expected)
  }

  test("loading unknown auth type results in error") {
    val config = ConfigFactory.parseString(
      """
        |    auth-type = "jwt"
        |    port = 21050
        |    schema = "default"
        |    ssl = true
        |""".stripMargin
    )

    val expected =
      Left(ConfDecodeErr("Received JDBC authentication type 'jwt' which is not supported"))
    val actual = ConnectionConfig.getFromConfig(config, "host")
    assert(actual == expected)
  }

}
