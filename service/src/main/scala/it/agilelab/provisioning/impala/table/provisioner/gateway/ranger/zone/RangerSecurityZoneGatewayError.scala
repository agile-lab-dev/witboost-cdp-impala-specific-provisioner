package it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.zone

import cats.Show
import cats.implicits._
import it.agilelab.provisioning.commons.client.ranger.RangerClientError
import it.agilelab.provisioning.commons.principalsmapping.PrincipalsMapperError

trait RangerSecurityZoneGatewayError //extends Exception with Product with Serializable

object RangerSecurityZoneGatewayError {
  final case class RangerSecurityZoneGatewayInitErr(error: RangerClientError)
      extends RangerSecurityZoneGatewayError
  final case class UpsertSecurityZoneErr(error: RangerClientError)
      extends RangerSecurityZoneGatewayError
  final case class FindSecurityZoneOwnerErr(error: RangerClientError)
      extends RangerSecurityZoneGatewayError
  final case class FindServiceErr(error: String) extends RangerSecurityZoneGatewayError
  final case class GenerateSecurityZoneErr(error: String) extends RangerSecurityZoneGatewayError
  final case class PrincipalMappingSecurityZoneErr(error: PrincipalsMapperError)
      extends RangerSecurityZoneGatewayError

  implicit def showRangerSecurityZoneGatewayError: Show[RangerSecurityZoneGatewayError] =
    Show.show {
      case RangerSecurityZoneGatewayInitErr(error) => show"RangerSecurityZoneGatewayInitErr($error)"
      case UpsertSecurityZoneErr(error)            => show"UpsertSecurityZoneErr($error)"
      case FindSecurityZoneOwnerErr(error)         => show"FindSecurityZoneOwnerErr($error)"
      case FindServiceErr(error)                   => show"FindServiceErr($error)"
      case GenerateSecurityZoneErr(error)          => show"GenerateSecurityZoneErr($error)"
      case PrincipalMappingSecurityZoneErr(error)  => show"PrincipalMappingSecurityZoneErr($error)"
    }
}
