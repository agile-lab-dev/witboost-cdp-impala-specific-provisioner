package it.agilelab.provisioning.impala.table.provisioner.clients.sql.ddl

import it.agilelab.provisioning.impala.table.provisioner.core.model.{
  ExternalTable,
  ImpalaEntity,
  ImpalaView
}

trait DataDefinitionLanguageProvider {

  def createDataBase(database: String, ifNotExists: Boolean): String

  def createExternalTable(externalTable: ExternalTable, ifNotExists: Boolean): String

  def createView(impalaView: ImpalaView, ifNotExists: Boolean): String

  def dropExternalTable(externalTable: ExternalTable, ifExists: Boolean): String

  def dropView(impalaView: ImpalaView, ifExists: Boolean): String

  def refreshStatements(impalaEntity: ImpalaEntity): Seq[String]

}
