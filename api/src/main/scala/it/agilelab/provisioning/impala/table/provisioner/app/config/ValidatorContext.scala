package it.agilelab.provisioning.impala.table.provisioner.app.config

import cats.implicits.toBifunctorOps
import it.agilelab.provisioning.aws.s3.gateway.S3Gateway
import it.agilelab.provisioning.commons.client.cdp.dw.CdpDwClient
import it.agilelab.provisioning.commons.client.cdp.env.CdpEnvClient
import it.agilelab.provisioning.commons.config.Conf
import it.agilelab.provisioning.commons.config.ConfError.ConfKeyNotFoundErr
import it.agilelab.provisioning.commons.http.Http
import it.agilelab.provisioning.impala.table.provisioner.app.api.validator.{
  CdpValidator,
  HDFSLocationValidator,
  LocationValidator,
  S3LocationValidator
}
import it.agilelab.provisioning.impala.table.provisioner.context.{
  ApplicationConfiguration,
  ContextError
}
import it.agilelab.provisioning.impala.table.provisioner.context.ContextError.{
  ClientError,
  ConfigurationError
}
import it.agilelab.provisioning.impala.table.provisioner.gateway.hdfs.HdfsClient

import scala.util.Try

final case class ValidatorContext(
    cdpValidator: CdpValidator,
    locationValidator: LocationValidator
)

final case class PrivateValidatorContext(locationValidator: LocationValidator)

object ValidatorContext {

  def initPublic(
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

  def initPrivate(conf: Conf): Either[ContextError, PrivateValidatorContext] = for {
    hdfsBaseUrl <- Try(
      ApplicationConfiguration.hdfsConfig.getString(
        ApplicationConfiguration.HDFS_BASE_URL)).toEither.leftMap(_ =>
      ConfigurationError(ConfKeyNotFoundErr(ApplicationConfiguration.HDFS_BASE_URL)))
  } yield PrivateValidatorContext(
    new HDFSLocationValidator(HdfsClient.defaultWithAudit(Http.defaultWithAudit(), hdfsBaseUrl)))

}
