package it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.provider

import cats.implicits._
import it.agilelab.provisioning.commons.client.ranger.RangerClient
import it.agilelab.provisioning.commons.http.Auth.BasicCredential
import it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.policy.RangerPolicyGateway
import it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.zone.RangerSecurityZoneGateway

import scala.util.Try

/** Class to create the gateways that communicate with the Cloudera Ranger to manage security zones and policies
  * Currently it uses a [[it.agilelab.provisioning.impala.table.provisioner.repository.RoleConfigRepository]]
  * to map between roles arriving from the descriptor and CDP roles.
  * In the future, this should be refactored to generate them automatically using naming conventions or using a better storage like PostgreSQL
  *
  * @param username Deploy user username with the appropriate permissions
  * @param password Deploy user password
  */
class RangerGatewayProvider(
    username: String,
    password: String
) {

  def getRangerClient(
      rangerHost: String
  ): Either[RangerGatewayProviderError, RangerClient] =
    Try(
      RangerClient.defaultWithAudit(
        prepRangerHost(rangerHost),
        BasicCredential(username, password))).toEither.leftMap(e => RangerGatewayProviderError(e))

  def getRangerPolicyGateway(
      rangerClient: RangerClient
  ): Either[RangerGatewayProviderError, RangerPolicyGateway] =
    Right(RangerPolicyGateway.rangerWithConfig(rangerClient))

  def getRangerSecurityZoneGateway(
      rangerClient: RangerClient
  ): Either[RangerGatewayProviderError, RangerSecurityZoneGateway] =
    Right(RangerSecurityZoneGateway.defaultWithConfig(rangerClient))

  protected def prepRangerHost(host: String): String =
    if (host.endsWith("/"))
      host.substring(0, host.length - 1)
    else host

}
