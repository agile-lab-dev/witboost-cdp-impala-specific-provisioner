package it.agilelab.provisioning.impala.table.provisioner.gateway.hdfs

import io.circe.Decoder
import it.agilelab.provisioning.commons.http.Auth.NoAuth
import it.agilelab.provisioning.commons.http.{ Auth, Http }
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite
import io.circe.generic.auto._
import it.agilelab.provisioning.commons.http.HttpErrors.{ ClientErr, ServerErr }
import it.agilelab.provisioning.impala.table.provisioner.gateway.hdfs.HdfsClientError.GetFolderStatusErr

class DefaultHdfsClientTest extends AnyFunSuite with MockFactory {

  val httpMock: Http = stub[Http]
  val baseUrl: String = "http://base-webhdfs:50070"
  val hdfsClient: DefaultHdfsClient = new DefaultHdfsClient(httpMock, baseUrl)

  test("get folder status returns Right(FileStatusesResponse)") {

    val fileStatusResponse = FileStatusesResponse(FileStatuses(Seq.empty))

    (httpMock
      .get[FileStatusesResponse](_: String, _: Map[String, String], _: Auth)(
        _: Decoder[FileStatusesResponse]))
      .when(
        "http://base-webhdfs:50070/webhdfs/v1/a/directory/path?op=LISTSTATUS",
        Map.empty[String, String],
        NoAuth(),
        *)
      .returns(
        Right(fileStatusResponse)
      )

    val expected = Right(Some(fileStatusResponse))
    val actual = hdfsClient.getFolderStatus("a/directory/path")
    assert(actual == expected)

  }

  test("get folder status returns Right(None)") {
    (httpMock
      .get[FileStatusesResponse](_: String, _: Map[String, String], _: Auth)(
        _: Decoder[FileStatusesResponse]))
      .when(
        "http://base-webhdfs:50070/webhdfs/v1/a/directory/path?op=LISTSTATUS",
        Map.empty[String, String],
        NoAuth(),
        *)
      .returns(
        Left(ClientErr(404, "Not found"))
      )

    val expected = Right(None)
    val actual = hdfsClient.getFolderStatus("a/directory/path")
    assert(actual == expected)

  }

  test("get folder status returns Left(err)") {
    val err = ServerErr(500, "Internal Server Error")

    (httpMock
      .get[FileStatusesResponse](_: String, _: Map[String, String], _: Auth)(
        _: Decoder[FileStatusesResponse]))
      .when(
        "http://base-webhdfs:50070/webhdfs/v1/a/directory/path?op=LISTSTATUS",
        Map.empty[String, String],
        NoAuth(),
        *)
      .returns(
        Left(err)
      )

    val expected = Left(GetFolderStatusErr(err))
    val actual = hdfsClient.getFolderStatus("a/directory/path")
    assert(actual == expected)

  }
}
