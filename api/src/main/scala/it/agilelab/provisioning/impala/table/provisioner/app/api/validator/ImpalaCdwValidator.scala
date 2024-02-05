package it.agilelab.provisioning.impala.table.provisioner.app.api.validator

import io.circe.generic.auto._
import io.circe.syntax._
import io.circe.{ Decoder, Json }
import it.agilelab.provisioning.commons.validator.Validator
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaCdw.ImpalaProvisionRequestOps
import it.agilelab.provisioning.impala.table.provisioner.core.model.{
  PrivateImpalaCdw,
  PublicImpalaCdw
}
import it.agilelab.provisioning.mesh.self.service.api.model.Component.OutputPort
import it.agilelab.provisioning.mesh.self.service.api.model.{ DataProduct, ProvisionRequest }

object ImpalaCdwValidator {
  def impalaCdwValidator(
      cdpValidator: CdpValidator,
      locationValidator: LocationValidator
  )(implicit decoder: Decoder[PublicImpalaCdw]): Validator[ProvisionRequest[Json, Json]] =
    Validator[ProvisionRequest[Json, Json]]
      .rule(
        r =>
          r.component match {
            case Some(_) => true
            case None    => false
          },
        _ => "Missing component to be provisioned"
      )
      .rule(
        r =>
          r.component match {
            case Some(c: OutputPort[_]) => true
            case _                      => false
          },
        _ => "The provided component is not accepted by this provisioner"
      )
      .rule(
        r => withinReq[PublicImpalaCdw](r)((_, _) => true),
        _ => "The provided component's specific section does not match the structure expected by this provisioner"
      )
      .rule(
        r =>
          withinReq[PublicImpalaCdw](r) { (_, op) =>
            cdpValidator.cdpEnvironmentExists(op.specific.cdpEnvironment)
          },
        _ => s"CDP Environment does not exist"
      )
      .rule(
        r =>
          withinReq[PublicImpalaCdw](r) { (_, op) =>
            cdpValidator.cdwVirtualClusterExists(
              op.specific.cdpEnvironment,
              op.specific.cdwVirtualWarehouse)

          },
        _ => s"CDW Virtual Warehouse does not exist in the specified environment"
      )
      .rule(
        r =>
          withinReq[PublicImpalaCdw](r) { (_, op) =>
            locationValidator.isValidLocation(op.specific.location)
          },
        _ => "Location is wrongly formatted. Correct location should be formatted as \"s3a://$bucket/$pathToFolder/\""
      )
      .rule(
        r =>
          withinReq[PublicImpalaCdw](r) { (_, op) =>
            locationValidator.locationExists(op.specific.location)
          },
        _ => s"Location does not exist on provided Amazon S3 bucket"
      )
      .rule(
        {
          case ProvisionRequest(_, Some(c: OutputPort[Json])) =>
            SchemaValidator.nonEmptySchema(c.dataContract.schema)
          case _ => false
        },
        _ => s"Schema is empty"
      )
      .rule(
        {
          case ProvisionRequest(_, Some(c: OutputPort[Json])) =>
            SchemaValidator.validateColumnNames(c.dataContract.schema)
          case _ => false
        },
        _ => s"Schema contains duplicated and/or empty column names"
      )
      .rule(
        {
          case ProvisionRequest(_, Some(c: OutputPort[Json])) =>
            SchemaValidator.isValidSchema(c.dataContract.schema)
          case _ => false
        },
        _ => s"Schema is not compliant with Impala Data Types specifications"
      )
      .rule(
        r =>
          withinReq[PublicImpalaCdw](r) { (_, op) =>
            SchemaValidator
              .arePartitionsValid(op.dataContract.schema, op.specific.partitions)
          },
        _ =>
          s"Partitions are not valid. Column does not exist on the contract schema or its type is not valid for partitioning"
      )

  def privateImpalaCdwValidator(
      locationValidator: LocationValidator
  ): Validator[ProvisionRequest[Json, Json]] =
    Validator[ProvisionRequest[Json, Json]]
      .rule(
        r =>
          r.component match {
            case Some(_) => true
            case None    => false
          },
        _ => "Missing component to be provisioned"
      )
      .rule(
        r =>
          r.component match {
            case Some(c: OutputPort[_]) => true
            case _                      => false
          },
        _ => "The provided component is not accepted by this provisioner"
      )
      .rule(
        r => withinReq[PrivateImpalaCdw](r)((_, _) => true),
        _ => "The provided component specific is not accepted by this provisioner"
      )
      .rule(
        r =>
          withinReq[PrivateImpalaCdw](r) { (_, op) =>
            locationValidator.isValidLocation(op.specific.location)
          },
        _ => "Location is wrongly formatted. Correct location should be formatted as \"/$pathToFolder\""
      )
      // Commented as we don't have authentication configured for Kerberos (See WIT-1364)
      /*.rule(
        r =>
          withinReq[PrivateImpalaCdw](r) { (_, op) =>
            locationValidator.locationExists(op.specific.location)
          },
        _ => "Location does not exist on HDFS"
      )
       */
      .rule(
        {
          case ProvisionRequest(_, Some(c: OutputPort[Json])) =>
            SchemaValidator.nonEmptySchema(c.dataContract.schema)
          case _ => false
        },
        _ => s"Schema is empty"
      )
      .rule(
        {
          case ProvisionRequest(_, Some(c: OutputPort[Json])) =>
            SchemaValidator.validateColumnNames(c.dataContract.schema)
          case _ => false
        },
        _ => s"Schema contains duplicated and/or empty column names"
      )
      .rule(
        {
          case ProvisionRequest(_, Some(c: OutputPort[Json])) =>
            SchemaValidator.isValidSchema(c.dataContract.schema)
          case _ => false
        },
        _ => s"Schema is not compliant with Impala Data Types specifications"
      )
      .rule(
        r =>
          withinReq[PrivateImpalaCdw](r) { (_, op) =>
            SchemaValidator
              .arePartitionsValid(op.dataContract.schema, op.specific.partitions)
          },
        _ =>
          s"Partitions are not valid. Column does not exist on the contract schema or its type is not valid for partitioning"
      )

  private def withinReq[COMP_SPEC](
      provisionRequest: ProvisionRequest[Json, Json]
  )(
      check: (DataProduct[Json], OutputPort[COMP_SPEC]) => Boolean
  )(implicit decoder: Decoder[COMP_SPEC]): Boolean =
    provisionRequest.getOutputPortRequest[COMP_SPEC] match {
      case Right(op) => check(provisionRequest.dataProduct, op)
      case Left(_)   => false
    }

}
