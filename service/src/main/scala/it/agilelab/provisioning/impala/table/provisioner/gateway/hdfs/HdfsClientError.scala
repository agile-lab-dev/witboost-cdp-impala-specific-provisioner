package it.agilelab.provisioning.impala.table.provisioner.gateway.hdfs

import cats.Show
import cats.implicits._
import it.agilelab.provisioning.commons.http.HttpErrors

sealed trait HdfsClientError extends Product with Serializable
object HdfsClientError {
  final case class GetFolderStatusErr(error: HttpErrors) extends HdfsClientError

  implicit val showHdfsClientError: Show[HdfsClientError] = Show.show {
    case GetFolderStatusErr(error) => show"GetFolderStatusErr($error)"
  }
}
