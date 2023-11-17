package it.agilelab.provisioning.impala.table.provisioner.clients.cdp

import cats.implicits.toBifunctorOps
import cats.implicits.catsSyntaxEq
import com.cloudera.cdp.datalake.model.{ Datalake, DatalakeDetails }
import com.cloudera.cdp.environments.model.Environment
import it.agilelab.provisioning.commons.client.cdp.dl.CdpDlClient
import it.agilelab.provisioning.commons.client.cdp.env.CdpEnvClient
import HostProviderError.{
  DataLakeClientErr,
  DataLakeNotFoundError,
  EnvironmentClientErr,
  GetImpalaCoordinatorErr,
  GetRangerHostErr
}

import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.Try

class HostProvider(cdpEnvClient: CdpEnvClient, cdpDlClient: CdpDlClient) {
  private val FIND_DL_ERR = "Unable to find dl cluster"

  def getEnvironment(environment: String): Either[HostProviderError, Environment] =
    for {
      env <- cdpEnvClient
        .describeEnvironment(environment)
        .leftMap(e => EnvironmentClientErr(e))
    } yield env

  def getDataLake(env: Environment): Either[HostProviderError, Datalake] =
    for {
      dls <- cdpDlClient.findAllDl().leftMap(e => DataLakeClientErr(e))
      dl  <- dls.find(_.getEnvironmentCrn === env.getCrn).toRight(DataLakeNotFoundError(FIND_DL_ERR))
    } yield dl

  def getImpalaCoordinatorHost(
      env: Environment,
      virtualCluster: String
  ): Either[HostProviderError, String] =
    Try {
      val freeIpaDomain = env.getFreeipa.getDomain
      val domain = freeIpaDomain.substring(freeIpaDomain.indexOf('.') + 1)
      "coordinator-%s.dw-%s.%s".format(virtualCluster, env.getEnvironmentName, domain)
    }.toEither.leftMap(e => GetImpalaCoordinatorErr(e))

  def getRangerHost(dl: Datalake): Either[HostProviderError, String] =
    for {
      dlDesc <- cdpDlClient
        .describeDl(dl.getDatalakeName)
        .leftMap(e => DataLakeClientErr(e))
      rangerEndpoint <- retrieveRangerEndpoint(dlDesc)
      rangerHost     <- extractRangerHost(rangerEndpoint)
    } yield rangerHost

  private def retrieveRangerEndpoint(dl: DatalakeDetails): Either[HostProviderError, String] =
    dl.getEndpoints.getEndpoints.asScala
      .find(e => e.getServiceName === "RANGER_ADMIN" && e.getMode === "PAM")
      .map(_.getServiceUrl)
      .toRight(GetRangerHostErr("Unable to find ranger admin endpoint"))

  private def extractRangerHost(rangerHost: String): Either[HostProviderError, String] =
    rangerHost match {
      case s"http://$path"  => Right(path)
      case s"https://$path" => Right(path)
      case _                => Left(GetRangerHostErr("Unable to extract ranger endpoint"))
    }
}
