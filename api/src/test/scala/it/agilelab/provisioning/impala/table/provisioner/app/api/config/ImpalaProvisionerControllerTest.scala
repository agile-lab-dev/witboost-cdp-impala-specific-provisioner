package it.agilelab.provisioning.impala.table.provisioner.app.api.config

import cats.implicits.toShow
import com.typesafe.config.{ Config, ConfigFactory }
import it.agilelab.provisioning.commons.config.{ Conf, ConfError }
import it.agilelab.provisioning.impala.table.provisioner.app.config.ImpalaProvisionerController
import it.agilelab.provisioning.impala.table.provisioner.context.ContextError
import org.scalatest.funsuite.AnyFunSuite

class ImpalaProvisionerControllerTest extends AnyFunSuite {

  test("private provisioner controller") {
    // see src/test/resources/application.conf
    val actual = ImpalaProvisionerController(new Conf {
      override def get(key: String): Either[ConfError, String] = Right("value")
      override def getConfig(key: String): Either[ConfError, Config] = Right(ConfigFactory.empty())
    })

    assert(actual.isRight)
  }

}
