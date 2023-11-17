package it.agilelab.provisioning.impala.table.provisioner.app.api.validator

import it.agilelab.provisioning.commons.validator.Validator
import it.agilelab.provisioning.mesh.self.service.api.model.Component.OutputPort
import it.agilelab.provisioning.mesh.self.service.api.model.ProvisionRequest
import io.circe.Json
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaCdw

object ImpalaCdwValidator {
  def impalaCdwValidator(
      cdpValidator: CdpValidator,
      locationValidator: LocationValidator
  ): Validator[ProvisionRequest[Json, ImpalaCdw]] =
    Validator[ProvisionRequest[Json, ImpalaCdw]]
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
            case Some(_: OutputPort[ImpalaCdw]) => true
            case _                              => false
          },
        _ => "The provided component is not accepted by this provisioner"
      )
      .rule(
        {
          case ProvisionRequest(_, Some(c: OutputPort[ImpalaCdw])) =>
            cdpValidator.cdpEnvironmentExists(c.specific.cdpEnvironment)
          case _ => false
        },
        _ => s"CDP Environment does not exist"
      )
      .rule(
        {
          case ProvisionRequest(_, Some(c: OutputPort[ImpalaCdw])) =>
            cdpValidator.cdwVirtualClusterExists(
              c.specific.cdpEnvironment,
              c.specific.cdwVirtualWarehouse)
          case _ => false
        },
        _ => s"CDW Virtual Warehouse does not exist in the specified environment"
      )
      .rule(
        {
          case ProvisionRequest(_, Some(c: OutputPort[ImpalaCdw])) =>
            locationValidator.locationExists(c.specific.location)
          case _ => false
        },
        _ =>
          s"Location does not exists or is wrongly formatted; " +
            "correct location should be formatted as \"s3a://$bucket/$pathToFolder/\""
      )
      .rule(
        {
          case ProvisionRequest(_, Some(c: OutputPort[ImpalaCdw])) =>
            SchemaValidator.validateColumnNames(c.dataContract.schema)
          case _ => false
        },
        _ => s"Schema contains duplicated and/or empty column names"
      )
      .rule(
        {
          case ProvisionRequest(_, Some(c: OutputPort[ImpalaCdw])) =>
            SchemaValidator.isValidSchema(c.dataContract.schema)
          case _ => false
        },
        _ => s"Schema is not compliant with Impala Data Types specifications"
      )
      .rule(
        {
          case ProvisionRequest(_, Some(c: OutputPort[ImpalaCdw])) =>
            SchemaValidator.arePartitionsValid(c.dataContract.schema, c.specific.partitions)
          case _ => false
        },
        _ =>
          s"Partitions are not valid. Column does not exist on the contract schema or its type is not valid for partitioning"
      )
      .rule(
        {
          case ProvisionRequest(dp, Some(c: OutputPort[ImpalaCdw])) =>
            NameValidator.isDatabaseNameValid(dp, c)
          case _ => false
        },
        {
          case ProvisionRequest(dp, Some(_: OutputPort[ImpalaCdw])) =>
            s"Impala Database name must be equal to ${NameValidator.getDatabaseName(dp)}"
          case _ =>
            "Impala Database name must be equal to the data product id and must contains only the characters [a-zA-Z0-9_]"
        }
      )
      .rule(
        {
          case ProvisionRequest(dp, Some(c: OutputPort[ImpalaCdw])) =>
            NameValidator.isTableNameValid(dp, c)
          case _ => false
        },
        {
          case ProvisionRequest(dp, Some(c: OutputPort[ImpalaCdw])) =>
            s"Impala Table name must be equal to ${NameValidator.getTableName(dp, c)}"
          case _ =>
            "Impala Table name must be equal to $ComponentId_$Environment and must contains only the characters [a-zA-Z0-9_]"
        }
      )
}
