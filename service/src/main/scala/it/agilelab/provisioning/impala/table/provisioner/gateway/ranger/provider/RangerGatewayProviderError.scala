package it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.provider

import cats.Show
import cats.implicits._
import it.agilelab.provisioning.commons.showable.ShowableOps.showThrowableError

final case class RangerGatewayProviderError(error: Throwable) extends Exception

object RangerGatewayProviderError {
  implicit def showRangerGatewayProviderError: Show[RangerGatewayProviderError] =
    Show.show(e => show"RangerGatewayProviderError(${e.error})")
}
