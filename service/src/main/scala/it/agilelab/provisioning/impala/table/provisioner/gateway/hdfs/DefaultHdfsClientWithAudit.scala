package it.agilelab.provisioning.impala.table.provisioner.gateway.hdfs

import cats.implicits.showInterpolator
import it.agilelab.provisioning.commons.audit.Audit

class DefaultHdfsClientWithAudit(hdfsClient: HdfsClient, audit: Audit) extends HdfsClient {
  private val INFO_MSG = "Executing %s"

  override def getFolderStatus(
      path: String
  ): Either[HdfsClientError, Option[FileStatusesResponse]] = {
    val action = s"GetFolderStatus($path)"
    audit.info(INFO_MSG.format(action))
    val result = hdfsClient.getFolderStatus(path)
    auditWithinResult(result, action)
    result
  }

  private def auditWithinResult[D](
      result: Either[HdfsClientError, D],
      action: String
  ): Unit =
    result match {
      case Right(_) => audit.info(show"$action completed successfully")
      case Left(l)  => audit.error(show"$action failed. Details: $l")
    }
}
