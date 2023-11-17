package it.agilelab.provisioning.impala.table.provisioner.clients.cdp

import com.cloudera.cdp.datalake.model.{ Datalake, DatalakeDetails, Endpoint, Endpoints }
import com.cloudera.cdp.dw.model.ClusterSummary
import com.cloudera.cdp.environments.model.{ Environment, FreeipaDetails }
import it.agilelab.provisioning.commons.client.cdp.dl.CdpDlClient
import it.agilelab.provisioning.commons.client.cdp.env.CdpEnvClient
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

import java.util

class HostProviderTest extends AnyFunSuite with MockFactory {

  test("getEnvironment") {
    val environment = new Environment()
    environment.setEnvironmentName("env1")

    val cdpEnvClient = stub[CdpEnvClient]
    val cdpDlClient = stub[CdpDlClient]
    (cdpEnvClient.describeEnvironment _).when("env1").returns(Right(environment))

    val cdpClient = new HostProvider(cdpEnvClient, cdpDlClient)
    val actual = cdpClient.getEnvironment("env1")
    val expected = Right(environment)
    assert(actual == expected)
  }

  test("getDataLake") {
    val environment = new Environment()
    environment.setCrn("env1")

    val datalake = new Datalake()
    datalake.setEnvironmentCrn("env1")
    datalake.setDatalakeName("dlName")

    val cdpEnvClient = stub[CdpEnvClient]
    val cdpDlClient = stub[CdpDlClient]
    (cdpDlClient.findAllDl _).when().returns(Right(Seq(datalake)))

    val cdpClient = new HostProvider(cdpEnvClient, cdpDlClient)
    val actual = cdpClient.getDataLake(environment)
    val expected = Right(datalake)
    assert(actual == expected)
  }

  test("getImpalaCoordinatorHost") {
    val freeipaDetails = new FreeipaDetails()
    freeipaDetails.setDomain("sdp-aw-d.ysuy-npya.cloudera.site")

    val environment = new Environment()
    environment.setFreeipa(freeipaDetails)
    environment.setCrn("y")
    environment.setEnvironmentName("env1")

    val cdpEnvClient = stub[CdpEnvClient]
    val cdpDlClient = stub[CdpDlClient]
    (cdpEnvClient.describeEnvironment _).when(*).returns(Right(environment))

    val cdpClient = new HostProvider(cdpEnvClient, cdpDlClient)
    val actual = cdpClient.getImpalaCoordinatorHost(environment, "cluster2")
    val expected = Right("coordinator-cluster2.dw-env1.ysuy-npya.cloudera.site")
    assert(actual == expected)
  }

  test("getRangerHost") {
    val environment = new Environment()
    environment.setCrn("x")

    val endpoint1 = new Endpoint()
    val endpoint2 = new Endpoint()
    endpoint1.setServiceName("x")
    endpoint2.setServiceName("RANGER_ADMIN")
    endpoint2.setMode("PAM")
    endpoint2.setServiceUrl("http://test/host/")

    val list = new util.ArrayList[Endpoint]()
    list.add(endpoint1)
    list.add(endpoint2)

    val endpoints = new Endpoints()
    endpoints.setEndpoints(list)

    val datalake = new Datalake()
    datalake.setEnvironmentCrn("x")
    datalake.setDatalakeName("dlName")

    val datalakeDetails = new DatalakeDetails()
    datalakeDetails.setEndpoints(endpoints)

    val cdpEnvClient = stub[CdpEnvClient]
    val cdpDlClient = stub[CdpDlClient]

    val cdpClient = new HostProvider(cdpEnvClient, cdpDlClient)
    (cdpEnvClient.describeEnvironment _).when(*).returns(Right(environment))
    (cdpDlClient.findAllDl _).when().returns(Right(Seq(datalake)))
    (cdpDlClient.describeDl _).when("dlName").returns(Right(datalakeDetails))

    val actual = cdpClient.getRangerHost(datalake)
    val expected = Right("test/host/")
    assert(actual == expected)

  }
}
