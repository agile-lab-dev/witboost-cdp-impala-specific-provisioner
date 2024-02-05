package it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.provider

import cats.implicits._
import it.agilelab.provisioning.commons.client.ranger.RangerClient
import it.agilelab.provisioning.commons.http.Auth.BasicCredential
import it.agilelab.provisioning.impala.table.provisioner.context.ApplicationConfiguration
import it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.policy.RangerPolicyGateway
import it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.role.RangerRoleGateway
import it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.zone.RangerSecurityZoneGateway

import scala.util.Try

/** Class to create the gateways that communicate with the Cloudera Ranger to manage security zones and policies
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
    Try {
      ApplicationConfiguration.rangerConfig.getString(
        // TODO We should create an enumeration in the scala-mesh-commons library to manage this error handling
        ApplicationConfiguration.RANGER_AUTH_TYPE) match {
        case RangerClient.SIMPLE_AUTH =>
          RangerClient.defaultWithAudit(rangerHost, BasicCredential(username, password))
        case RangerClient.KERBEROS_AUTH =>
          RangerClient.defaultWithKerberosWithAudit(rangerHost, username, password)
        case e => throw new NoSuchElementException(s"Authentication type $e is not supported")
      }
    }.toEither.leftMap(e => RangerGatewayProviderError(e))

  def getRangerGateways(rangerHost: String): Either[RangerGatewayProviderError, RangerGateway] =
    for {
      rangerClient <- getRangerClient(rangerHost)
      gateway      <- getRangerGatewaysFromClient(rangerClient)
    } yield gateway

  def getRangerGatewaysFromClient(
      rangerClient: RangerClient
  ): Either[RangerGatewayProviderError, RangerGateway] =
    for {
      rangerSecZoneGateway <- getRangerSecurityZoneGateway(rangerClient)
      policyGateway        <- getRangerPolicyGateway(rangerClient)
      roleGateway          <- getRangerRoleGateway(rangerClient)
    } yield new RangerGateway(policyGateway, rangerSecZoneGateway, roleGateway)

  def getRangerPolicyGateway(
      rangerClient: RangerClient
  ): Either[RangerGatewayProviderError, RangerPolicyGateway] =
    Right(RangerPolicyGateway.ranger(rangerClient))

  def getRangerSecurityZoneGateway(
      rangerClient: RangerClient
  ): Either[RangerGatewayProviderError, RangerSecurityZoneGateway] =
    Right(RangerSecurityZoneGateway.default(rangerClient))

  def getRangerRoleGateway(
      rangerClient: RangerClient
  ): Either[RangerGatewayProviderError, RangerRoleGateway] =
    Right(RangerRoleGateway.default(rangerClient))

}
