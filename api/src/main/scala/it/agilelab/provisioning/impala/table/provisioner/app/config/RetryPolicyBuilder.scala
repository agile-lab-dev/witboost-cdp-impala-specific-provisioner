package it.agilelab.provisioning.impala.table.provisioner.app.config

import cats.Applicative
import cats.implicits.toBifunctorOps
import com.typesafe.config.Config
import it.agilelab.provisioning.commons.config.ConfError.ConfKeyNotFoundErr
import it.agilelab.provisioning.impala.table.provisioner.context.ApplicationConfiguration
import it.agilelab.provisioning.impala.table.provisioner.context.ContextError.ConfigurationError
import retry.{ RetryPolicies, RetryPolicy }

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

object RetryPolicyBuilder {

  def applyFromConfig[F[_]: Applicative](
      config: Config
  ): Either[ConfigurationError, RetryPolicy[F]] =
    for {
      maxRetries <- Try(config.getInt(ApplicationConfiguration.maxRetries)).toEither.leftMap(_ =>
        ConfigurationError(ConfKeyNotFoundErr(ApplicationConfiguration.maxRetries)))
      backoffRetry <- Try(config.getDuration(ApplicationConfiguration.exponentialBackoff)).toEither
        .leftMap(_ =>
          ConfigurationError(ConfKeyNotFoundErr(ApplicationConfiguration.exponentialBackoff)))
    } yield RetryPolicies
      .limitRetries[F](maxRetries)
      .join(RetryPolicies.exponentialBackoff[F](
        FiniteDuration(backoffRetry.toMillis, TimeUnit.MILLISECONDS)))
}
