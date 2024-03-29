package it.agilelab.provisioning.impala.table.provisioner.app.api.validator

import it.agilelab.provisioning.aws.s3.gateway.S3Gateway
import it.agilelab.provisioning.impala.table.provisioner.gateway.hdfs.HdfsClient

import java.nio.file.Paths

trait LocationValidator {
  def locationExists(location: String): Boolean
  def isValidLocation(location: String): Boolean
}
class S3LocationValidator(s3Gateway: S3Gateway) extends LocationValidator {
  def locationExists(location: String): Boolean = {
    val result = for {
      bucketAndKey <- getBucketAndKey(location)
      content      <- s3Gateway.listObjects(bucketAndKey._1, Some(bucketAndKey._2))
    } yield content
    result match {
      case Right(list) => list.nonEmpty
      case Left(_)     => false
    }
  }

  private def getBucketAndKey(location: String): Either[String, (String, String)] = location match {
    case s"s3a://$bucket/$path/" => Right((bucket, s"$path/"))
    case _                       => Left("Wrongly formatted path")
  }

  override def isValidLocation(location: String): Boolean = getBucketAndKey(location).isRight
}

class HDFSLocationValidator(hdfsClient: HdfsClient) extends LocationValidator {
  override def locationExists(location: String): Boolean = (for {
    content <- hdfsClient.getFolderStatus(location)
  } yield content) match {
    case Right(list) => list.nonEmpty
    case Left(_)     => false
  }

  override def isValidLocation(location: String): Boolean = location.startsWith("/")
}
