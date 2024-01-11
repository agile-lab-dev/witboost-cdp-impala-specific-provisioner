package it.agilelab.provisioning.impala.table.provisioner.app.api.validator

import io.circe.Json
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaCdw
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaFormat.Parquet
import it.agilelab.provisioning.mesh.self.service.api.model.Component.{ DataContract, OutputPort }
import it.agilelab.provisioning.mesh.self.service.api.model.DataProduct
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

class NameValidatorTest extends AnyFunSuite with MockFactory {
  test("isDatabaseNameValid return true") {
    val actual = NameValidator.isDatabaseNameValid(
      DataProduct[Json](
        id = "urn:dmb:dp:domain:dataProductName:0",
        name = "dataProductName",
        domain = "domain",
        environment = "environment",
        version = "0.0.0",
        dataProductOwner = "dataProductOwner",
        devGroup = "devGroup",
        ownerGroup = "ownerGroup",
        specific = Json.obj(),
        components = Seq.empty
      ),
      OutputPort[ImpalaCdw](
        id = "urn:dmb:cmp:domain:dataProductName:0:outputPortName",
        name = "outputPortName",
        description = "description",
        version = "0.0.0",
        dataContract = DataContract(
          schema = Seq.empty
        ),
        specific = ImpalaCdw(
          databaseName = "domain_dataProductName_0",
          tableName = "tableName",
          cdpEnvironment = "cdpEnvironment",
          cdwVirtualWarehouse = "cdwVirtualWarehouse",
          format = Parquet,
          location = "location",
          partitions = None
        )
      )
    )
    assert(actual)
  }

  test("isDatabaseNameValid return false") {
    val actual = NameValidator.isDatabaseNameValid(
      DataProduct[Json](
        id = "urn:dmb:dp:domain:dataProductName:0",
        name = "dataProductName",
        domain = "domain",
        environment = "environment",
        version = "0.0.0",
        dataProductOwner = "dataProductOwner",
        devGroup = "devGroup",
        ownerGroup = "ownerGroup",
        specific = Json.obj(),
        components = Seq.empty
      ),
      OutputPort[ImpalaCdw](
        id = "urn:dmb:cmp:domain:dataProductName:0:outputPortName",
        name = "outputPortName",
        description = "description",
        version = "0.0.0",
        dataContract = DataContract(
          schema = Seq.empty
        ),
        specific = ImpalaCdw(
          databaseName = "databaseName",
          tableName = "tableName",
          cdpEnvironment = "cdpEnvironment",
          cdwVirtualWarehouse = "cdwVirtualWarehouse",
          format = Parquet,
          location = "location",
          partitions = None
        )
      )
    )
    assert(!actual)
  }

  test("isTableNameValid return true") {
    val actual = NameValidator.isTableNameValid(
      DataProduct[Json](
        id = "urn:dmb:dp:domain:dataProductName:0",
        name = "dataProductName",
        domain = "domain",
        environment = "environment",
        version = "0.0.0",
        dataProductOwner = "dataProductOwner",
        devGroup = "devGroup",
        ownerGroup = "ownerGroup",
        specific = Json.obj(),
        components = Seq.empty
      ),
      OutputPort[ImpalaCdw](
        id = "urn:dmb:cmp:domain:dataProductName:0:outputPortName",
        name = "outputPortName",
        description = "description",
        version = "0.0.0",
        dataContract = DataContract(
          schema = Seq.empty
        ),
        specific = ImpalaCdw(
          databaseName = "databaseName",
          tableName = "domain_dataProductName_0_outputPortName_environment",
          cdpEnvironment = "cdpEnvironment",
          cdwVirtualWarehouse = "cdwVirtualWarehouse",
          format = Parquet,
          location = "location",
          partitions = None
        )
      )
    )
    assert(actual)
  }

  test("isTableNameValid return false") {
    val actual = NameValidator.isTableNameValid(
      DataProduct[Json](
        id = "urn:dmb:dp:domain:dataProductName:0",
        name = "dataProductName",
        domain = "domain",
        environment = "environment",
        version = "0.0.0",
        dataProductOwner = "dataProductOwner",
        devGroup = "devGroup",
        ownerGroup = "ownerGroup",
        specific = Json.obj(),
        components = Seq.empty
      ),
      OutputPort[ImpalaCdw](
        id = "urn:dmb:cmp:domain:dataProductName:0:outputPortName",
        name = "outputPortName",
        description = "description",
        version = "0.0.0",
        dataContract = DataContract(
          schema = Seq.empty
        ),
        specific = ImpalaCdw(
          databaseName = "databaseName",
          tableName = "tableName",
          cdpEnvironment = "cdpEnvironment",
          cdwVirtualWarehouse = "cdwVirtualWarehouse",
          format = Parquet,
          location = "location",
          partitions = None
        )
      )
    )
    assert(!actual)
  }
}
