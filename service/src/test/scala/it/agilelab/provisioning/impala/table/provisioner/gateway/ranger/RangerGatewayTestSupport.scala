package it.agilelab.provisioning.impala.table.provisioner.gateway.ranger

import it.agilelab.provisioning.commons.client.ranger.RangerClientError.{
  CreateSecurityZoneErr,
  FindSecurityZoneByNameErr,
  UpdateSecurityZoneErr
}
import it.agilelab.provisioning.commons.client.ranger.model.RangerSecurityZone
import it.agilelab.provisioning.commons.http.HttpErrors.ConnectionErr
import it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.zone.RangerSecurityZoneGatewayError
import it.agilelab.provisioning.impala.table.provisioner.gateway.ranger.zone.RangerSecurityZoneGatewayError.UpsertSecurityZoneErr
import org.scalatest.EitherValues._

trait RangerGatewayTestSupport {
  def assertUpsertZoneWithFindZoneErr[A](
      actual: Either[RangerSecurityZoneGatewayError, A],
      zoneName: String,
      httpError: ConnectionErr
  ): Unit = {
    assert(actual.isLeft)
    assert(actual.left.value.isInstanceOf[UpsertSecurityZoneErr])
    assert(
      actual.left.value
        .asInstanceOf[UpsertSecurityZoneErr]
        .error
        .isInstanceOf[FindSecurityZoneByNameErr])
    assert(
      actual.left.value
        .asInstanceOf[UpsertSecurityZoneErr]
        .error
        .asInstanceOf[FindSecurityZoneByNameErr]
        .securityZoneName == zoneName
    )
    assert(
      actual.left.value
        .asInstanceOf[UpsertSecurityZoneErr]
        .error
        .asInstanceOf[FindSecurityZoneByNameErr]
        .error
        .isInstanceOf[ConnectionErr]
    )
    assert(
      actual.left.value
        .asInstanceOf[UpsertSecurityZoneErr]
        .error
        .asInstanceOf[FindSecurityZoneByNameErr]
        .error
        .asInstanceOf[ConnectionErr]
        .getMessage == httpError.getMessage
    )
  }

  def assertUpsertZoneWithUpdateZoneErr[A](
      actual: Either[RangerSecurityZoneGatewayError, A],
      zone: RangerSecurityZone,
      httpError: ConnectionErr
  ): Unit = {
    assert(actual.isLeft)
    assert(actual.left.value.isInstanceOf[UpsertSecurityZoneErr])
    assert(
      actual.left.value
        .asInstanceOf[UpsertSecurityZoneErr]
        .error
        .isInstanceOf[UpdateSecurityZoneErr])
    assert(
      actual.left.value
        .asInstanceOf[UpsertSecurityZoneErr]
        .error
        .asInstanceOf[UpdateSecurityZoneErr]
        .securityZone == zone
    )
    assert(
      actual.left.value
        .asInstanceOf[UpsertSecurityZoneErr]
        .error
        .asInstanceOf[UpdateSecurityZoneErr]
        .error
        .isInstanceOf[ConnectionErr]
    )
    assert(
      actual.left.value
        .asInstanceOf[UpsertSecurityZoneErr]
        .error
        .asInstanceOf[UpdateSecurityZoneErr]
        .error
        .asInstanceOf[ConnectionErr]
        .getMessage == httpError.getMessage
    )
  }

  def assertUpsertZoneWithCreateZoneErr[A](
      actual: Either[RangerSecurityZoneGatewayError, A],
      zone: RangerSecurityZone,
      httpError: ConnectionErr
  ): Unit = {
    assert(actual.isLeft)
    assert(actual.left.value.isInstanceOf[UpsertSecurityZoneErr])
    assert(
      actual.left.value
        .asInstanceOf[UpsertSecurityZoneErr]
        .error
        .isInstanceOf[CreateSecurityZoneErr])
    assert(
      actual.left.value
        .asInstanceOf[UpsertSecurityZoneErr]
        .error
        .asInstanceOf[CreateSecurityZoneErr]
        .securityZone == zone
    )
    assert(
      actual.left.value
        .asInstanceOf[UpsertSecurityZoneErr]
        .error
        .asInstanceOf[CreateSecurityZoneErr]
        .error
        .isInstanceOf[ConnectionErr]
    )
    assert(
      actual.left.value
        .asInstanceOf[UpsertSecurityZoneErr]
        .error
        .asInstanceOf[CreateSecurityZoneErr]
        .error
        .asInstanceOf[ConnectionErr]
        .getMessage == httpError.getMessage
    )
  }

}
