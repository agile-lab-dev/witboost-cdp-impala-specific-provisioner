package it.agilelab.provisioning.impala.table.provisioner.app.api.validator

import it.agilelab.provisioning.mesh.self.service.api.model.Component.OutputPort
import it.agilelab.provisioning.mesh.self.service.api.model.DataProduct
import io.circe.Json
import it.agilelab.provisioning.impala.table.provisioner.core.model.{
  ImpalaCdw,
  ImpalaTableCdw,
  ImpalaViewCdw,
  PrivateImpalaViewCdw
}

object NameValidator {

  def isDatabaseNameValid[COMP_SPEC <: ImpalaCdw](
      dataProduct: DataProduct[Json],
      component: OutputPort[COMP_SPEC]
  ): Boolean =
    getDatabaseName(dataProduct).equals(component.specific.databaseName)

  def isTableNameValid[COMP_SPEC <: ImpalaCdw](
      dataProduct: DataProduct[Json],
      component: OutputPort[COMP_SPEC]
  ): Boolean = component.specific match {
    case sp: ImpalaTableCdw       => getTableName(dataProduct, component).equals(sp.tableName)
    case sp: PrivateImpalaViewCdw => getTableName(dataProduct, component).equals(sp.viewName)
    case _                        => true
  }

  def getDatabaseName(dataProduct: DataProduct[Json]): String =
    sanitizeCDWString(extractDataProductId(dataProduct))

  def getTableName[COMP_SPEC <: ImpalaCdw](
      dataProduct: DataProduct[Json],
      component: OutputPort[COMP_SPEC]
  ): String =
    sanitizeCDWString(
      String.join("_", extractComponentId[COMP_SPEC](component), dataProduct.environment))

  private def sanitizeCDWString(s: String): String =
    s.replaceAll("[^a-zA-Z0-9_]", "_")

  private def extractComponentId[COMP_SPEC <: ImpalaCdw](component: OutputPort[COMP_SPEC]): String =
    component.id match {
      case s"urn:dmb:cmp:$id" => id
      case a                  => a
    }

  private def extractDataProductId(dataProduct: DataProduct[Json]): String = dataProduct.id match {
    case s"urn:dmb:dp:$id" => id
    case a                 => a
  }
}
