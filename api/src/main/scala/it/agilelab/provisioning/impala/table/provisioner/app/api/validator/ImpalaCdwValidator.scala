package it.agilelab.provisioning.impala.table.provisioner.app.api.validator

import cats.data.Validated.Invalid
import cats.data.{ NonEmptyList, ValidatedNel }
import io.circe.{ Decoder, Json }
import it.agilelab.provisioning.commons.validator.{ ValidationFail, Validator, ValidatorError }
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaCdw.ImpalaProvisionRequestOps
import it.agilelab.provisioning.mesh.self.service.api.model.Component.{
  OutputPort,
  StorageArea,
  Workload
}
import it.agilelab.provisioning.mesh.self.service.api.model.{ DataProduct, ProvisionRequest }

class ImpalaCdwValidator(
    outputPortValidator: Validator[ProvisionRequest[Json, Json]],
    storageAreaValidator: Validator[ProvisionRequest[Json, Json]]
) extends Validator[ProvisionRequest[Json, Json]] {
  override def validate(
      entity: ProvisionRequest[Json, Json]
  ): Either[ValidatorError[ProvisionRequest[Json, Json]], ValidatedNel[ValidationFail[
    ProvisionRequest[Json, Json]
  ], ProvisionRequest[Json, Json]]] = entity.component match {
    case Some(_: OutputPort[Json])  => outputPortValidator.validate(entity)
    case Some(_: StorageArea[Json]) => storageAreaValidator.validate(entity)
    case Some(_: Workload[_]) =>
      Right(
        Invalid(
          NonEmptyList.one(
            ValidationFail(
              entity,
              "Received kind 'workload' which is not supported by the provisioner"))))
    case None =>
      Right(
        Invalid(
          NonEmptyList.one(
            ValidationFail(
              entity,
              "Received provisioning request does not contain a component to deploy"))))
  }
}

object ImpalaCdwValidator {
  def impalaCdwValidator(
      cdpValidator: CdpValidator,
      locationValidator: LocationValidator
  ): Validator[ProvisionRequest[Json, Json]] =
    new ImpalaCdwValidator(
      ImpalaOutputPortValidator.outputPortImpalaCdwValidator(cdpValidator, locationValidator),
      ImpalaStorageAreaValidator.storageAreaImpalaCdwValidator(cdpValidator, locationValidator)
    )

  def privateImpalaCdwValidator(
      locationValidator: LocationValidator
  ): Validator[ProvisionRequest[Json, Json]] =
    new ImpalaCdwValidator(
      ImpalaOutputPortValidator.privateOutputPortImpalaCdwValidator(locationValidator),
      ImpalaStorageAreaValidator.privateStorageAreaImpalaCdwValidator(locationValidator)
    )

  def withinOutputPortReq[COMP_SPEC](
      provisionRequest: ProvisionRequest[Json, Json]
  )(
      check: (DataProduct[Json], OutputPort[COMP_SPEC]) => Boolean
  )(implicit decoder: Decoder[COMP_SPEC]): Boolean =
    provisionRequest.getOutputPortRequest[COMP_SPEC] match {
      case Right(op) => check(provisionRequest.dataProduct, op)
      case Left(_)   => false
    }

  def withinStorageAreaReq[COMP_SPEC](
      provisionRequest: ProvisionRequest[Json, Json]
  )(
      check: (DataProduct[Json], StorageArea[COMP_SPEC]) => Boolean
  )(implicit decoder: Decoder[COMP_SPEC]): Boolean =
    provisionRequest.getStorageAreaRequest[COMP_SPEC] match {
      case Right(op) => check(provisionRequest.dataProduct, op)
      case Left(_)   => false
    }

}
