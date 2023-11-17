package it.agilelab.provisioning.impala.table.provisioner.app.api.validator

import it.agilelab.provisioning.aws.s3.gateway.S3Gateway
import it.agilelab.provisioning.aws.s3.gateway.S3GatewayError.GetObjectContentErr
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

class LocationValidatorTest extends AnyFunSuite with MockFactory {
  val s3Gateway = mock[S3Gateway]
  val locationValidator = new LocationValidator(s3Gateway)

  test("location validation return false when path is wrongly formatted") {
    assert(!locationValidator.locationExists("path"))
  }

  test("location validation return false when getObjectContent return Left") {
    (s3Gateway.getObjectContent _)
      .expects("bucket", "path/to/folder/")
      .returns(
        Left(GetObjectContentErr("bucket", "path/to/folder", new IllegalArgumentException("x"))))
    assert(!locationValidator.locationExists("s3a://bucket/path/to/folder/"))
  }

  test("location validation return true") {
    (s3Gateway.getObjectContent _)
      .expects("bucket", "path/to/folder/")
      .returns(Right(Array.emptyByteArray))
    assert(locationValidator.locationExists("s3a://bucket/path/to/folder/"))
  }
}
