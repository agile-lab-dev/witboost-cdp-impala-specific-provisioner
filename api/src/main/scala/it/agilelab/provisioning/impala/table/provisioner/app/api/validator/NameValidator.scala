package it.agilelab.provisioning.impala.table.provisioner.app.api.validator

import it.agilelab.provisioning.mesh.self.service.api.model.Component.OutputPort
import it.agilelab.provisioning.mesh.self.service.api.model.DataProduct
import io.circe.Json
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaCdw

object NameValidator {

  def isDatabaseNameValid(
      dataProduct: DataProduct[Json],
      component: OutputPort[ImpalaCdw]
  ): Boolean =
    getDatabaseName(dataProduct).equals(component.specific.databaseName)

  def isTableNameValid(dataProduct: DataProduct[Json], component: OutputPort[ImpalaCdw]): Boolean =
    getTableName(dataProduct, component).equals(component.specific.tableName)

  def getDatabaseName(dataProduct: DataProduct[Json]): String =
    sanitizeCDWString(extractDataProductId(dataProduct))

  def getTableName(
      dataProduct: DataProduct[Json],
      component: OutputPort[ImpalaCdw]
  ): String =
    sanitizeCDWString(String.join("_", extractComponentId(component), dataProduct.environment))

  private def sanitizeCDWString(s: String): String =
    s.replaceAll("[^a-zA-Z0-9_]", "_")

  private def extractComponentId(component: OutputPort[ImpalaCdw]): String = component.id match {
    case s"urn:dmb:cmp:$id" => id
    case a                  => a
  }

  private def extractDataProductId(dataProduct: DataProduct[Json]): String = dataProduct.id match {
    case s"urn:dmb:dp:$id" => id
    case a                 => a
  }
}
