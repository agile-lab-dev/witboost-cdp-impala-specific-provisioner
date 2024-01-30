package it.agilelab.provisioning.impala.table.provisioner.app.config

import cats.implicits.toBifunctorOps
import it.agilelab.provisioning.aws.s3.gateway.S3Gateway
import it.agilelab.provisioning.commons.client.cdp.dw.CdpDwClient
import it.agilelab.provisioning.commons.client.cdp.env.CdpEnvClient
import it.agilelab.provisioning.commons.config.Conf
import it.agilelab.provisioning.impala.table.provisioner.app.api.validator.{
  CdpValidator,
  LocationValidator,
  S3LocationValidator
}
import it.agilelab.provisioning.impala.table.provisioner.context.ContextError
import it.agilelab.provisioning.impala.table.provisioner.context.ContextError.ClientError

final case class ValidatorContext(
    cdpValidator: CdpValidator,
    locationValidator: LocationValidator
)

final case class PrivateValidatorContext(locationValidator: LocationValidator)

object ValidatorContext {

  // Unused variables since we're not implementing Provisioning Status (async requests) and role mapping with DB yet
  private val PROVISION_STATE_TABLE = "PROVISION_STATE_TABLE"
  private val PROVISION_STATE_TABLE_KEY = "PROVISION_STATE_TABLE_KEY"
  private val DATA_MESH_ROLE_TABLE = "DATA_MESH_ROLE_TABLE"
  private val DATA_MESH_ROLE_TABLE_KEY = "DATA_MESH_ROLE_TABLE_KEY"

  def init(
      conf: Conf
  ): Either[ContextError, ValidatorContext] =
    for {
      cdpEnvClient <- CdpEnvClient
        .defaultWithAudit()
        .leftMap(e => ClientError("CdpEnvClient", e))
      cdpDwClient <- CdpDwClient
        .defaultWithAudit()
        .leftMap(e => ClientError("CdpDwClient", e))
      s3Gateway <- S3Gateway
        .defaultWithAudit()
        .leftMap(e => ClientError("S3Gateway", e))
    } yield new ValidatorContext(
      new CdpValidator(cdpEnvClient, cdpDwClient),
      new S3LocationValidator(s3Gateway)
    )
}
