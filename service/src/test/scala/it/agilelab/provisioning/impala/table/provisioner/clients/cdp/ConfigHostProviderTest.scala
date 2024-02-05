package it.agilelab.provisioning.impala.table.provisioner.clients.cdp

import it.agilelab.provisioning.impala.table.provisioner.context.ApplicationConfiguration
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar

import scala.util.Random

class ConfigHostProviderTest extends AnyFunSuite with MockitoSugar {

  test("get ranger host") {
    val configHostProvider = new ConfigHostProvider()

    val expected = Right("https://ranger.internal.com/ranger/")
    val actual = configHostProvider.getRangerHost

    assert(expected == actual)
  }

  test("get impala host") {
    val mockRandom = mock[Random]
    when(mockRandom.nextInt(any[Int])).thenReturn(2)
    val configHostProvider = new ConfigHostProvider()

    val expected = Right("coordinator-3.impala.com")
    val actual = configHostProvider.getImpalaCoordinatorHost(mockRandom)

    assert(expected == actual)
  }

  test("get any impala host") {
    val configHostProvider = new ConfigHostProvider()

    val expected = ApplicationConfiguration.impalaConfig.getStringList(
      ApplicationConfiguration.COORDINATOR_HOST_URLS)
    val actual = configHostProvider.getImpalaCoordinatorHost()
    actual match {
      case Right(value) => assert(expected.contains(value))
      case Left(_)      => fail()
    }
  }
}
