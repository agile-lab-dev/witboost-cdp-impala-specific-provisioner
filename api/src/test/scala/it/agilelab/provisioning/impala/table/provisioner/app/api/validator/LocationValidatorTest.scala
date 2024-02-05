package it.agilelab.provisioning.impala.table.provisioner.app.api.validator

import it.agilelab.provisioning.aws.s3.gateway.S3Gateway
import it.agilelab.provisioning.aws.s3.gateway.S3GatewayError.ListObjectsErr
import it.agilelab.provisioning.commons.http.HttpErrors.ClientErr
import it.agilelab.provisioning.impala.table.provisioner.gateway.hdfs.HdfsClientError.GetFolderStatusErr
import it.agilelab.provisioning.impala.table.provisioner.gateway.hdfs.{
  DefaultHdfsClient,
  FileStatuses,
  FileStatusesResponse
}
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite
import software.amazon.awssdk.services.s3.model.S3Object

class LocationValidatorTest extends AnyFunSuite with MockFactory {
  val s3Gateway = mock[S3Gateway]
  val locationValidator = new S3LocationValidator(s3Gateway)

  test("location validation return false when path is wrongly formatted") {
    assert(!locationValidator.locationExists("path"))
  }

  test("location validation return false when listObjects return Left") {
    (s3Gateway.listObjects _)
      .expects("bucket", Some("path/to/folder/"))
      .returns(Left(ListObjectsErr("bucket", "path/to/folder", new IllegalArgumentException("x"))))
    assert(!locationValidator.locationExists("s3a://bucket/path/to/folder/"))
  }

  test("location validation return false if list is empty") {
    (s3Gateway.listObjects _)
      .expects("bucket", Some("path/to/folder/"))
      .returns(Right(Iterator.empty))
    assert(!locationValidator.locationExists("s3a://bucket/path/to/folder/"))
  }

  test("location validation return true") {
    (s3Gateway.listObjects _)
      .expects("bucket", Some("path/to/folder/"))
      .returns(Right(Iterator(S3Object.builder().key("bucket/path/to/folder/").build())))
    assert(locationValidator.locationExists("s3a://bucket/path/to/folder/"))
  }

  test("location hdfs exists") {

    val hdfsClient = mock[DefaultHdfsClient]

    (hdfsClient.getFolderStatus _)
      .expects("/mesh/warehouse/tablespace/external/hive/parquet-test-sergio")
      .returns(Right(Some(FileStatusesResponse(FileStatuses(Seq.empty)))))

    val validator =
      new HDFSLocationValidator(hdfsClient)
    assert(validator.locationExists("/mesh/warehouse/tablespace/external/hive/parquet-test-sergio"))
  }

  test("location hdfs doesn't exist") {

    val hdfsClient = mock[DefaultHdfsClient]

    (hdfsClient.getFolderStatus _)
      .expects("/mesh/warehouse/tablespace/external/hive/parquet-test-sergio")
      .returns(Left(GetFolderStatusErr(ClientErr(401, "Not authorized"))))

    val validator =
      new HDFSLocationValidator(hdfsClient)
    assert(
      !validator.locationExists("/mesh/warehouse/tablespace/external/hive/parquet-test-sergio"))
  }
}
