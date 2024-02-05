package it.agilelab.provisioning.impala.table.provisioner.gateway.hdfs

import it.agilelab.provisioning.commons.http.Http
import org.scalatest.funsuite.AnyFunSuite

class HdfsClientTest extends AnyFunSuite {

  test("default hdfs client") {
    val expected = HdfsClient.default(Http.defaultWithAudit(), "baseurl")
    assert(expected.isInstanceOf[DefaultHdfsClient])
  }

  test("default hdfs client with audit") {
    val expected = HdfsClient.defaultWithAudit(Http.defaultWithAudit(), "baseurl")
    assert(expected.isInstanceOf[DefaultHdfsClientWithAudit])
  }

}
