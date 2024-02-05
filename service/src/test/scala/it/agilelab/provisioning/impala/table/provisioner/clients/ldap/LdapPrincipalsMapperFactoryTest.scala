package it.agilelab.provisioning.impala.table.provisioner.clients.ldap

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

import scala.util.{ Failure, Success }

class LdapPrincipalsMapperFactoryTest extends AnyFunSuite {
  val factory = new LdapPrincipalsMapperFactory

  test("ldap config identifier") {
    assert(factory.configIdentifier.equals("ldap"))
  }

  test("factory fails creating mapper") {
    val mapper = factory.create(ConfigFactory.parseString("""{"anyConfig":"error"}"""))
    mapper match {
      case Success(_) => fail()
      case Failure(_) => ()
    }
  }

}
