package it.agilelab.provisioning.impala.table.provisioner.gateway.hdfs

import io.circe.generic.auto._
import it.agilelab.provisioning.commons.http.Auth.NoAuth
import it.agilelab.provisioning.commons.http.Http
import it.agilelab.provisioning.commons.http.HttpErrors.ClientErr
import it.agilelab.provisioning.impala.table.provisioner.gateway.hdfs.HdfsClientError.GetFolderStatusErr

class DefaultHdfsClient(http: Http, webHdfsBaseUrl: String) extends HdfsClient {

  val WEB_HDFS_BASE_ENDPOINT: String = s"$webHdfsBaseUrl/webhdfs/v1"

  def getFolderStatus(path: String): Either[HdfsClientError, Option[FileStatusesResponse]] =
    http.get[FileStatusesResponse](
      s"$WEB_HDFS_BASE_ENDPOINT/$path?op=LISTSTATUS",
      Map.empty,
      NoAuth()) match {
      case Right(r)                => Right(Some(r))
      case Left(ClientErr(404, _)) => Right(None)
      case Left(e)                 => Left(GetFolderStatusErr(e))
    }
}
