package it.agilelab.provisioning.impala.table.provisioner.app.api.config

import it.agilelab.provisioning.commons.config.Conf
import it.agilelab.provisioning.commons.http.{ DefaultHttp, Http }
import it.agilelab.provisioning.impala.table.provisioner.app.api.validator.HDFSLocationValidator
import it.agilelab.provisioning.impala.table.provisioner.app.config.{
  PrivateValidatorContext,
  ValidatorContext
}
import it.agilelab.provisioning.impala.table.provisioner.gateway.hdfs.DefaultHdfsClient
import org.scalatest.funsuite.AnyFunSuite

class ValidatorContextTest extends AnyFunSuite {
  test("initPrivate") {
    val actual = ValidatorContext.initPrivate(Conf.env())
    assert(actual.isRight)
  }
}
