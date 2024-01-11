package it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.policy

import cats.Show
import cats.implicits._
import it.agilelab.provisioning.commons.client.ranger.RangerClientError
import it.agilelab.provisioning.mesh.repository.RepositoryError

trait RangerPolicyGatewayError extends Exception with Product with Serializable

object RangerPolicyGatewayError {
  final case class RangerPolicyGatewayInitErr(error: RepositoryError)
      extends RangerPolicyGatewayError
  final case class AttachRangerPolicyErr(error: RepositoryError) extends RangerPolicyGatewayError
  final case class UpsertPolicyErr(error: RangerClientError) extends RangerPolicyGatewayError
  final case class DeletePolicyErr(error: RangerClientError) extends RangerPolicyGatewayError

  implicit def showPolicyGatewayError: Show[RangerPolicyGatewayError] = Show.show {
    case RangerPolicyGatewayInitErr(error) => show"RangerPolicyGatewayInitErr($error)"
    case AttachRangerPolicyErr(error)      => show"AttachRangerPolicyErr($error)"
    case UpsertPolicyErr(error)            => show"UpsertPolicyErr($error)"
    case DeletePolicyErr(error)            => show"DeletePolicyErr($error)"
  }
}
