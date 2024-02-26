package it.agilelab.provisioning.impala.table.provisioner.app.api.validator

import cats.data.NonEmptyList
import cats.data.Validated.Invalid
import io.circe.Json
import it.agilelab.provisioning.commons.validator.{ ValidationFail, Validator }
import it.agilelab.provisioning.impala.table.provisioner.app.api.validator.ImpalaCdwValidator.withinStorageAreaReq
import it.agilelab.provisioning.impala.table.provisioner.core.model.PrivateImpalaStorageAreaCdw
import it.agilelab.provisioning.mesh.self.service.api.model.Component.StorageArea
import it.agilelab.provisioning.mesh.self.service.api.model.ProvisionRequest
import io.circe.generic.auto._

object ImpalaStorageAreaValidator {
  def storageAreaImpalaCdwValidator(
      cdpValidator: CdpValidator,
      locationValidator: LocationValidator
  ): Validator[ProvisionRequest[Json, Json]] = obj =>
    Right(
      Invalid(
        NonEmptyList.one(
          ValidationFail(
            obj,
            "The provisioner currently doesn't support storage areas on CDP Public Cloud"))))
  /*
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
          case Some(c: StorageArea[_]) => true
          case _                       => false
        },
      _ => "The provided component is not accepted by this provisioner"
    )
    .rule(
      r => withinStorageAreaReq[ImpalaStorageAreaCdw](r)((_, _) => true),
      _ => "The provided component's specific section does not match the structure expected by this provisioner"
    )
    .rule(
      r =>
        withinStorageAreaReq[ImpalaStorageAreaCdw](r) { (_, sa) =>
          cdpValidator.cdpEnvironmentExists(sa.specific.cdpEnvironment)
        },
      _ => s"CDP Environment does not exist"
    )
    .rule(
      r =>
        withinStorageAreaReq[ImpalaStorageAreaCdw](r) { (_, sa) =>
          cdpValidator.cdwVirtualClusterExists(
            sa.specific.cdpEnvironment,
            sa.specific.cdwVirtualWarehouse)

        },
      _ => s"CDW Virtual Warehouse does not exist in the specified environment"
    )
    .rule(
      r =>
        withinStorageAreaReq[ImpalaStorageAreaCdw](r) { (_, sa) =>
          locationValidator.isValidLocation(sa.specific.location)
        },
      _ => "Location is wrongly formatted. Correct location should be formatted as \"s3a://$bucket/$pathToFolder/\""
    )
    .rule(
      r =>
        withinStorageAreaReq[ImpalaStorageAreaCdw](r) { (_, sa) =>
          locationValidator.locationExists(sa.specific.location)
        },
      _ => s"Location does not exist on provided Amazon S3 bucket"
    )
    .rule(
      r =>
        withinStorageAreaReq[ImpalaStorageAreaCdw](r) { (_, sa) =>
          SchemaValidator.nonEmptySchema(sa.specific.tableSchema)
        },
      _ => s"Schema is empty"
    )
    .rule(
      r =>
        withinStorageAreaReq[ImpalaStorageAreaCdw](r) { (_, sa) =>
          SchemaValidator.validateColumnNames(sa.specific.tableSchema)
        },
      _ => s"Schema contains duplicated and/or empty column names"
    )
    .rule(
      r =>
        withinStorageAreaReq[ImpalaStorageAreaCdw](r) { (_, sa) =>
          SchemaValidator.isValidSchema(sa.specific.tableSchema)
        },
      _ => s"Schema is not compliant with Impala Data Types specifications"
    )
    .rule(
      r =>
        withinStorageAreaReq[ImpalaStorageAreaCdw](r) { (_, sa) =>
          SchemaValidator
            .arePartitionsValid(sa.specific.tableSchema, sa.specific.partitions)
        },
      _ =>
        s"Partitions are not valid. Column does not exist on the contract schema or its type is not valid for partitioning"
    )
  }
   */

  def privateStorageAreaImpalaCdwValidator(
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
            case Some(c: StorageArea[_]) => true
            case _                       => false
          },
        _ => "The provided component is not accepted by this provisioner"
      )
      .rule(
        r => withinStorageAreaReq[PrivateImpalaStorageAreaCdw](r)((_, _) => true),
        _ => "The provided component specific is not accepted by this provisioner"
      )
      .rule(
        r =>
          withinStorageAreaReq[PrivateImpalaStorageAreaCdw](r) { (_, sa) =>
            locationValidator.isValidLocation(sa.specific.location)
          },
        _ => "Location is wrongly formatted. Correct location should be formatted as \"/$pathToFolder\""
      )
      // Commented as we don't have authentication configured for Kerberos (See WIT-1364)
      /*.rule(
      r =>
        withinStorageAreaReq[ImpalaStorageAreaCdw](r) { (_, sa) =>
          locationValidator.locationExists(sa.specific.location)
        },
      _ => "Location does not exist on HDFS"
    )
       */
      .rule(
        r =>
          withinStorageAreaReq[PrivateImpalaStorageAreaCdw](r) { (_, sa) =>
            SchemaValidator.nonEmptySchema(sa.specific.tableSchema)
          },
        _ => s"Schema is empty"
      )
      .rule(
        r =>
          withinStorageAreaReq[PrivateImpalaStorageAreaCdw](r) { (_, sa) =>
            SchemaValidator.validateColumnNames(sa.specific.tableSchema)
          },
        _ => s"Schema contains duplicated and/or empty column names"
      )
      .rule(
        r =>
          withinStorageAreaReq[PrivateImpalaStorageAreaCdw](r) { (_, sa) =>
            SchemaValidator.isValidSchema(sa.specific.tableSchema)
          },
        _ => s"Schema is not compliant with Impala Data Types specifications"
      )
      .rule(
        r =>
          withinStorageAreaReq[PrivateImpalaStorageAreaCdw](r) { (_, sa) =>
            SchemaValidator
              .arePartitionsValid(sa.specific.tableSchema, sa.specific.partitions)
          },
        _ =>
          s"Partitions are not valid. Column does not exist on the contract schema or its type is not valid for partitioning"
      )

}
