package it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection

import it.agilelab.provisioning.impala.table.provisioner.clients.sql.connection.pattern.ConnectionStringPatterns
import org.scalatest.funsuite.AnyFunSuite

class ConnectionStringPatternsTest extends AnyFunSuite {

  test("ensure required features about the uid/pwd connection string pattern") {
    val pattern = "jdbc:impala://%s:%s/(.*)".r
    ConnectionStringPatterns.impala match {
      case pattern(parameters) =>
        val params = parameters.split(";")
        assert(params.contains("AuthMech=3"))
        assert(params.contains("ssl=%s"))
        assert(params.contains("UID=%s"))
        assert(params.contains("PWD=%s"))
      case _ => fail()
    }
  }

  test("ensure required features about the kerberos connection string pattern") {
    val pattern = "jdbc:impala://%s:%s/(.*)".r
    ConnectionStringPatterns.kerberizedImpala match {
      case pattern(parameters) =>
        val params = parameters.split(";")
        assert(params.contains("AuthMech=1"))
        assert(params.contains("ssl=%s"))
        assert(params.contains("KrbHostFQDN=%s"))
        assert(params.contains("KrbServiceName=%s"))
      case _ => fail()
    }
  }

}
