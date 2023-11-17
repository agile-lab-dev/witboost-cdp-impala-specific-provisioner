package it.agilelab.provisioning.impala.table.provisioner.app.api.validator

import it.agilelab.provisioning.aws.s3.gateway.S3Gateway

class LocationValidator(s3Gateway: S3Gateway) {
  def locationExists(location: String): Boolean = {
    val result = for {
      bucketAndKey <- getBucketAndKey(location)
      content      <- s3Gateway.getObjectContent(bucketAndKey._1, bucketAndKey._2)
    } yield content
    result match {
      case Right(_) => true
      case Left(_)  => false
    }
  }

  private def getBucketAndKey(location: String): Either[String, (String, String)] = location match {
    case s"s3a://$bucket/$path/" => Right((bucket, s"$path/"))
    case _                       => Left("Wrongly formatted path")
  }

}
