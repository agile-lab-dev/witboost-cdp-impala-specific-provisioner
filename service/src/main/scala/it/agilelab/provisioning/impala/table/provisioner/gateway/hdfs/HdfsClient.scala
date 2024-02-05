package it.agilelab.provisioning.impala.table.provisioner.gateway.hdfs

import it.agilelab.provisioning.commons.audit.Audit
import it.agilelab.provisioning.commons.http.Http

final case class FileStatus(
    accessTime: Long,
    blockSize: Long,
    group: String,
    length: Long,
    modificationTime: Long,
    owner: String,
    pathSuffix: String,
    permission: String,
    replication: Long,
    `type`: String
)
final case class FileStatuses(FileStatus: Seq[FileStatus])
final case class FileStatusesResponse(FileStatuses: FileStatuses)

trait HdfsClient {
  def getFolderStatus(path: String): Either[HdfsClientError, Option[FileStatusesResponse]]
}

object HdfsClient {

  def default(http: Http, webHdfsBaseUrl: String): HdfsClient =
    new DefaultHdfsClient(http, webHdfsBaseUrl)

  def defaultWithAudit(http: Http, webHdfsBaseUrl: String): HdfsClient =
    new DefaultHdfsClientWithAudit(
      HdfsClient.default(http, webHdfsBaseUrl),
      Audit.default("HdfsClient")
    )
}
