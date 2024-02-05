package it.agilelab.provisioning.impala.table.provisioner.clients.cdp

import cats.implicits.toBifunctorOps
import it.agilelab.provisioning.impala.table.provisioner.clients.cdp.HostProviderError.{
  GetImpalaCoordinatorErr,
  GetRangerHostErr
}
import it.agilelab.provisioning.impala.table.provisioner.context.ApplicationConfiguration

import scala.util.{ Random, Try }

class ConfigHostProvider {

  /** Returns a coordinator host from the list of configured hosts. It uses a random strategy to return one of the available hosts
    * @return Left([[GetImpalaCoordinatorErr]]) if the list of coordinator hosts configuration wasn't found,
    *         or Right(String) with the chosen coordinator host
    */
  def getImpalaCoordinatorHost(random: Random = Random.self): Either[HostProviderError, String] =
    Try {
      val l = ApplicationConfiguration.impalaConfig.getStringList(
        ApplicationConfiguration.COORDINATOR_HOST_URLS)
      l.get(random.nextInt(l.size))
    }.toEither.leftMap(e => GetImpalaCoordinatorErr(e))

  /** Returns the Ranger host set in configuration
    * @return Left([[GetRangerHostErr]] if the ranger host configuration wasn't found,
    *         or Right(String) with the ranger host
    */
  def getRangerHost: Either[HostProviderError, String] = Try {
    ApplicationConfiguration.rangerConfig.getString(ApplicationConfiguration.RANGER_API_ENDPOINT)
  }.toEither.leftMap(e =>
    GetRangerHostErr(s"Cannot find Ranger host configuration: ${e.getMessage}"))

}
