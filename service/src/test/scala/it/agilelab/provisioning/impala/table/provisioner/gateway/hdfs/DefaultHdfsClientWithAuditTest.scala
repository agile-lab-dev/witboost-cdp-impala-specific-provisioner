package it.agilelab.provisioning.impala.table.provisioner.gateway.hdfs

import it.agilelab.provisioning.commons.audit.Audit
import it.agilelab.provisioning.commons.http.HttpErrors.ClientErr
import it.agilelab.provisioning.impala.table.provisioner.gateway.hdfs.HdfsClientError.GetFolderStatusErr
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

class DefaultHdfsClientWithAuditTest extends AnyFunSuite with MockFactory {

  val defaultHdfsClient: DefaultHdfsClient = stub[DefaultHdfsClient]
  val audit: Audit = mock[Audit]
  val hdfsClient = new DefaultHdfsClientWithAudit(defaultHdfsClient, audit)

  test("get folder status logs success info") {
    val fileStatusResponse = FileStatusesResponse(FileStatuses(Seq.empty))

    (defaultHdfsClient.getFolderStatus _).when(*).returns(Right(Some(fileStatusResponse)))
    inSequence(
      (audit.info _).expects("Executing GetFolderStatus(a/folder/path)"),
      (audit.info _).expects("GetFolderStatus(a/folder/path) completed successfully")
    )
    val actual = hdfsClient.getFolderStatus("a/folder/path")
    assert(actual == Right(Some(fileStatusResponse)))
  }

  test("get folder status logs error info") {

    (defaultHdfsClient.getFolderStatus _)
      .when(*)
      .returns(Left(GetFolderStatusErr(ClientErr(401, "x"))))
    inSequence(
      (audit.info _).expects("Executing GetFolderStatus(a/folder/path)"),
      (audit.error _).expects(
        "GetFolderStatus(a/folder/path) failed. Details: GetFolderStatusErr(ClientErr(401,x))")
    )
    val actual = hdfsClient.getFolderStatus("a/folder/path")
    assert(actual == Left(GetFolderStatusErr(ClientErr(401, "x"))))
  }

}
