package it.agilelab.provisioning.impala.table.provisioner.app.api.validator

import com.cloudera.cdp.dw.model.{ ClusterSummaryResponse, VwSummary }
import com.cloudera.cdp.environments.model.Environment
import it.agilelab.provisioning.commons.client.cdp.dw.CdpDwClient
import it.agilelab.provisioning.commons.client.cdp.dw.CdpDwClientError.{
  FindAllClustersErr,
  FindAllVwsErr
}
import it.agilelab.provisioning.commons.client.cdp.env.CdpEnvClient
import it.agilelab.provisioning.commons.client.cdp.env.CdpEnvClientError.DescribeEnvironmentErr
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

class CdpValidatorTest extends AnyFunSuite with MockFactory {
  val envClient: CdpEnvClient = stub[CdpEnvClient]
  val dwClient: CdpDwClient = stub[CdpDwClient]
  val validator = new CdpValidator(envClient, dwClient)

  test("cdpEnvironmentExists return true when describeEnvironment return Right") {
    (envClient.describeEnvironment _).when("envName").returns(Right(new Environment()))
    assert(validator.cdpEnvironmentExists("envName"))
  }

  test("cdpEnvironmentExists return false when describeEnvironment return Left") {
    (envClient.describeEnvironment _)
      .when("envName")
      .returns(Left(DescribeEnvironmentErr("envName", new IllegalArgumentException("x"))))
    assert(!validator.cdpEnvironmentExists("envName"))
  }

  test("cdwVirtualClusterExists return false when describeEnvironment return Left") {
    (envClient.describeEnvironment _)
      .when("envName")
      .returns(Left(DescribeEnvironmentErr("envName", new IllegalArgumentException("x"))))
    assert(!validator.cdwVirtualClusterExists("envName", "vwName"))
  }

  test("cdwVirtualClusterExists return false when findClusterByEnvironmentCrn return Left") {
    val environment = new Environment()
    environment.setCrn("envCrn")
    (envClient.describeEnvironment _).when("envName").returns(Right(environment))
    (dwClient.findClusterByEnvironmentCrn _)
      .when("envCrn")
      .returns(Left(FindAllClustersErr(new IllegalArgumentException("x"))))
    assert(!validator.cdwVirtualClusterExists("envName", "vwName"))
  }

  test("cdwVirtualClusterExists return false when findClusterByEnvironmentCrn return Right(None)") {
    val environment = new Environment()
    environment.setCrn("envCrn")
    (envClient.describeEnvironment _).when("envName").returns(Right(environment))
    (dwClient.findClusterByEnvironmentCrn _).when("envCrn").returns(Right(None))
    assert(!validator.cdwVirtualClusterExists("envName", "vwName"))
  }

  test("cdwVirtualClusterExists return false when findVwByName return Left") {
    val environment = new Environment()
    environment.setCrn("envCrn")
    val cluster = new ClusterSummaryResponse()
    cluster.setId("clusterId")
    (envClient.describeEnvironment _).when("envName").returns(Right(environment))
    (dwClient.findClusterByEnvironmentCrn _).when("envCrn").returns(Right(Some(cluster)))
    (dwClient.findVwByName _)
      .when("clusterId", "vwName")
      .returns(Left(FindAllVwsErr("clusterId", new IllegalArgumentException("x"))))
    assert(!validator.cdwVirtualClusterExists("envName", "vwName"))
  }

  test("cdwVirtualClusterExists return false when findVwByName return Right(None)") {
    val environment = new Environment()
    environment.setCrn("envCrn")
    val cluster = new ClusterSummaryResponse()
    cluster.setId("clusterId")
    (envClient.describeEnvironment _).when("envName").returns(Right(environment))
    (dwClient.findClusterByEnvironmentCrn _).when("envCrn").returns(Right(Some(cluster)))
    (dwClient.findVwByName _).when("clusterId", "vwName").returns(Right(None))
    assert(!validator.cdwVirtualClusterExists("envName", "vwName"))
  }

  test("cdwVirtualClusterExists return true") {
    val environment = new Environment()
    environment.setCrn("envCrn")
    val cluster = new ClusterSummaryResponse()
    cluster.setId("clusterId")
    (envClient.describeEnvironment _).when("envName").returns(Right(environment))
    (dwClient.findClusterByEnvironmentCrn _).when("envCrn").returns(Right(Some(cluster)))
    (dwClient.findVwByName _).when("clusterId", "vwName").returns(Right(Some(new VwSummary())))
    assert(validator.cdwVirtualClusterExists("envName", "vwName"))
  }
}
