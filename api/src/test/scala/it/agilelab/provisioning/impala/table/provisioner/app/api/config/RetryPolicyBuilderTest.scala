package it.agilelab.provisioning.impala.table.provisioner.app.api.config

import cats.effect.IO
import cats.implicits.catsSyntaxSemigroup
import com.typesafe.config.ConfigFactory
import it.agilelab.provisioning.commons.config.ConfError.ConfKeyNotFoundErr
import it.agilelab.provisioning.impala.table.provisioner.app.config.RetryPolicyBuilder
import it.agilelab.provisioning.impala.table.provisioner.context.ApplicationConfiguration
import it.agilelab.provisioning.impala.table.provisioner.context.ContextError.ConfigurationError
import org.scalatest.funsuite.AnyFunSuite
import retry.RetryPolicies

import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.MapHasAsJava

class RetryPolicyBuilderTest extends AnyFunSuite {

  test("create a exponential backoff policy from valid config") {

    val config = ConfigFactory.parseMap(
      Map(
        "max-retries"         -> "5",
        "exponential-backoff" -> "500 milliseconds"
      ).asJava)

    val expected =
      RetryPolicies.limitRetries[IO](5) |+| RetryPolicies.exponentialBackoff[IO](500.milliseconds)

    val actual = RetryPolicyBuilder.applyFromConfig[IO](config)

    assert(actual match {
      case Right(policy) => policy.show == expected.show
      case Left(_)       => false
    })

  }

  test("failed create a exponential backoff policy from invalid config - maxRetries") {

    val config = ConfigFactory.parseMap(
      Map(
        "exponential-backoff" -> "500 milliseconds"
      ).asJava)

    val expected = Left(ConfigurationError(ConfKeyNotFoundErr(ApplicationConfiguration.maxRetries)))
    val actual = RetryPolicyBuilder.applyFromConfig[IO](config)

    assert(actual == expected)
  }

  test("failed create a exponential backoff policy from invalid config - exponentialBackoff") {

    val config = ConfigFactory.parseMap(
      Map(
        "max-retries" -> "5"
      ).asJava)

    val expected =
      Left(ConfigurationError(ConfKeyNotFoundErr(ApplicationConfiguration.exponentialBackoff)))
    val actual = RetryPolicyBuilder.applyFromConfig[IO](config)

    assert(actual == expected)
  }

}
