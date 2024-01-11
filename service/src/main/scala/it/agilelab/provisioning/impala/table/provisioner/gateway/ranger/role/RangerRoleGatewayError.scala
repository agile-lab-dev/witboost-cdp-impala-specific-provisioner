package it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.role

import cats.Show
import cats.implicits.showInterpolator
import it.agilelab.provisioning.commons.client.ranger.RangerClientError

trait RangerRoleGatewayError extends Exception with Product with Serializable

object RangerRoleGatewayError {

  final case class UpsertRoleErr(error: RangerClientError) extends RangerRoleGatewayError
  final case class DeleteRoleErr(error: RangerClientError) extends RangerRoleGatewayError
  final case class FindRoleErr(error: RangerClientError) extends RangerRoleGatewayError

  implicit def showPolicyGatewayError: Show[RangerRoleGatewayError] = Show.show {
    case UpsertRoleErr(error) => show"UpsertRoleErr($error)"
    case DeleteRoleErr(error) => show"DeleteRoleErr($error)"
    case FindRoleErr(error)   => show"FindRoleErr($error)"
  }
}
