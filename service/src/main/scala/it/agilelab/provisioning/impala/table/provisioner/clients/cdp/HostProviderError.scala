package it.agilelab.provisioning.impala.table.provisioner.clients.cdp

import cats.Show
import cats.implicits._
import it.agilelab.provisioning.commons.client.cdp.dl.CdpDlClientError
import it.agilelab.provisioning.commons.client.cdp.env.CdpEnvClientError
import it.agilelab.provisioning.commons.showable.ShowableOps

trait HostProviderError extends Exception with Product with Serializable

object HostProviderError {
  final case class EnvironmentClientErr(error: CdpEnvClientError) extends HostProviderError
  final case class DataLakeClientErr(error: CdpDlClientError) extends HostProviderError
  final case class DataLakeNotFoundError(error: String) extends HostProviderError
  final case class GetImpalaCoordinatorErr(error: Throwable) extends HostProviderError
  final case class GetRangerHostErr(error: String) extends HostProviderError

  implicit def showHostProviderError: Show[HostProviderError] = Show.show {
    case EnvironmentClientErr(error)  => show"GetEnvironmentErr($error)"
    case DataLakeClientErr(error)     => show"GetDataLakeErr($error)"
    case DataLakeNotFoundError(error) => show"DataLakeNotFoundError($error)"
    case GetImpalaCoordinatorErr(error) =>
      show"GetImpalaCoordinatorErr(${ShowableOps.showThrowableError.show(error)})"
    case GetRangerHostErr(error) => show"GetRangerHostErr($error)"
  }
}
