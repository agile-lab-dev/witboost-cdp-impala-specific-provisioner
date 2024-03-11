package it.agilelab.provisioning.impala.table.provisioner.app.api.validator

import io.circe.Json
import it.agilelab.provisioning.commons.validator.Validator
import it.agilelab.provisioning.impala.table.provisioner.app.api.validator.ImpalaCdwValidator.withinOutputPortReq
import it.agilelab.provisioning.impala.table.provisioner.core.model.{
  ImpalaCdw,
  PrivateImpalaTableCdw,
  PrivateImpalaViewCdw,
  PublicImpalaTableCdw,
  TableParams
}
import it.agilelab.provisioning.mesh.self.service.api.model.Component.OutputPort
import it.agilelab.provisioning.mesh.self.service.api.model.ProvisionRequest
import io.circe.generic.auto._
import it.agilelab.provisioning.impala.table.provisioner.gateway.mapper.ExternalTableMapper

object ImpalaOutputPortValidator {
  def outputPortImpalaCdwValidator(
      cdpValidator: CdpValidator,
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
        r => withinOutputPortReq[PublicImpalaTableCdw](r)((_, _) => true),
        _ => "The provided component's specific section does not match the structure expected by this provisioner"
      )
      .rule(
        r =>
          withinOutputPortReq[PublicImpalaTableCdw](r) { (_, op) =>
            cdpValidator.cdpEnvironmentExists(op.specific.cdpEnvironment)
          },
        _ => "CDP Environment does not exist"
      )
      .rule(
        r =>
          withinOutputPortReq[PublicImpalaTableCdw](r) { (_, op) =>
            cdpValidator.cdwVirtualClusterExists(
              op.specific.cdpEnvironment,
              op.specific.cdwVirtualWarehouse)

          },
        _ => "CDW Virtual Warehouse does not exist in the specified environment"
      )
      .rule(
        r =>
          withinOutputPortReq[PublicImpalaTableCdw](r) { (_, op) =>
            locationValidator.isValidLocation(op.specific.location)
          },
        _ => "Location is wrongly formatted. Correct location should be formatted as \"s3a://$bucket/$pathToFolder/\""
      )
      .rule(
        r =>
          withinOutputPortReq[PublicImpalaTableCdw](r) { (_, op) =>
            locationValidator.locationExists(op.specific.location)
          },
        _ => "Location does not exist on provided Amazon S3 bucket"
      )
      .rule(
        {
          case ProvisionRequest(_, Some(c: OutputPort[Json])) =>
            SchemaValidator.nonEmptySchema(c.dataContract.schema)
          case _ => false
        },
        _ => "Schema is empty"
      )
      .rule(
        {
          case ProvisionRequest(_, Some(c: OutputPort[Json])) =>
            SchemaValidator.validateColumnNames(c.dataContract.schema)
          case _ => false
        },
        _ => "Schema contains duplicated and/or empty column names"
      )
      .rule(
        {
          case ProvisionRequest(_, Some(c: OutputPort[Json])) =>
            SchemaValidator.isValidSchema(c.dataContract.schema)
          case _ => false
        },
        _ => "Schema is not compliant with Impala Data Types specifications"
      )
      .rule(
        r =>
          withinOutputPortReq[PublicImpalaTableCdw](r) { (_, op) =>
            SchemaValidator
              .arePartitionsValid(op.dataContract.schema, op.specific.partitions)
          },
        _ => "Partitions are not valid. Column does not exist on the contract schema or its type is not valid for partitioning"
      )
      .rule(
        r =>
          withinOutputPortReq[PublicImpalaTableCdw](r) { (_, op) =>
            op.specific.tableParams match {
              case Some(TableParams(_, Some(delimiter), _)) =>
                ExternalTableMapper.hexStringToByte(delimiter).isDefined
              case _ => true
            }
          },
        _ => "tableParams.delimiter is an unexpected value. It is not a hex number nor a single ASCII character"
      )

  def privateOutputPortImpalaCdwValidator(
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
        r =>
          withinOutputPortReq[ImpalaCdw](r)((_, op) =>
            op.specific match {
              case _: PrivateImpalaTableCdw | _: PrivateImpalaViewCdw => true
              case _                                                  => false
            }),
        _ => "The provided component specific is not accepted by this provisioner"
      )
      .rule(
        r =>
          withinOutputPortReq[ImpalaCdw](r) { (_, op) =>
            op.specific match {
              case sp: PrivateImpalaTableCdw => locationValidator.isValidLocation(sp.location)
              case _                         => true
            }
          },
        _ => "External table location is wrongly formatted. Correct location should be formatted as \"/$pathToFolder\""
      )
      // Commented as we don't have authentication configured for Kerberos (See WIT-1364)
      /*.rule(
        r =>
          withinOutputPortReq[PrivateImpalaCdw](r) { (_, op) =>
            locationValidator.locationExists(op.specific.location)
          },
        _ => "Location does not exist on HDFS"
      )
       */
      .rule(
        {
          case ProvisionRequest(_, Some(c: OutputPort[Json])) =>
            SchemaValidator.nonEmptySchema(c.dataContract.schema)
          case _ => true
        },
        _ => "Schema is empty"
      )
      .rule(
        {
          case ProvisionRequest(_, Some(c: OutputPort[Json])) =>
            SchemaValidator.validateColumnNames(c.dataContract.schema)
          case _ => true
        },
        _ => "Schema contains duplicated and/or empty column names"
      )
      .rule(
        {
          case ProvisionRequest(_, Some(c: OutputPort[Json])) =>
            SchemaValidator.isValidSchema(c.dataContract.schema)
          case _ => true
        },
        _ => "Schema is not compliant with Impala Data Types specifications"
      )
      .rule(
        r =>
          withinOutputPortReq[ImpalaCdw](r) { (_, op) =>
            op.specific match {
              case sp: PrivateImpalaTableCdw =>
                SchemaValidator.arePartitionsValid(op.dataContract.schema, sp.partitions)
              case _ => true
            }
          },
        _ => "External table partitions are not valid. Column does not exist on the contract schema or its type is not valid for partitioning"
      )
      .rule(
        r =>
          withinOutputPortReq[ImpalaCdw](r) { (_, op) =>
            op.specific match {
              case PrivateImpalaTableCdw(_, _, _, _, _, Some(TableParams(_, Some(delimiter), _))) =>
                ExternalTableMapper.hexStringToByte(delimiter).isDefined
              case _ => true
            }
          },
        _ => "tableParams.delimiter is an unexpected value. It is not a hex number nor a single ASCII character"
      )
}
