package it.agilelab.provisioning.impala.table.provisioner.clients.sql.ddl

import cats.implicits._
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaFormat.{ Csv, Parquet }
import it.agilelab.provisioning.impala.table.provisioner.core.model.{
  ExternalTable,
  Field,
  ImpalaEntity,
  ImpalaView
}

class ImpalaDataDefinitionLanguageProvider extends DataDefinitionLanguageProvider {

  private val CREATE_DATABASE_PATTERN = "%s %s"
  private val CREATE_TABLE_PATTERN = "%s %s (%s) %s%s"
  private val CREATE_VIEW_PATTERN = "%s %s AS %s"
  private val CREATE_PARTITIONED_TABLE_PATTERN = "%s %s (%s) PARTITIONED BY (%s) %s%s"
  private val DROP_TABLE_PATTERN = "%s %s"
  private val DROP_VIEW_PATTERN = "%s %s"

  private val CREATE_DATABASE_KEY = "CREATE DATABASE"
  private val CREATE_EXTERNAL_TABLE_KEY = "CREATE EXTERNAL TABLE"
  private val CREATE_VIEW_KEY = "CREATE VIEW"
  private val TBLPROPERTIES_KEY = "TBLPROPERTIES"
  private val IF_NOT_EXISTS_KEY = "IF NOT EXISTS"
  private val IF_EXISTS_KEY = "IF EXISTS"

  private val DROP_TABLE_KEY = "DROP TABLE"
  private val DROP_VIEW_KEY = "DROP VIEW"

  private val STORED_AS_PATTERN = "STORED AS %s"
  private val ROW_FORMAT_PATTERN = "ROW FORMAT %s"
  private val LOCATION_PATTERN = "LOCATION '%s'"

  private val SELECT_PATTERN = "SELECT %s"
  private val FROM_PATTERN = "FROM %s"

  private val defaultTblProperties: Seq[(String, String)] = Seq(("impala.disableHmsSync", "false"))

  override def createExternalTable(
      externalTable: ExternalTable,
      ifNotExists: Boolean
  ): String =
    externalTable match {
      case ExternalTable(_, _, _, partitions, _, _) if partitions.nonEmpty =>
        createPartitionedExternalTable(externalTable, ifNotExists)
      case _ => createDefaultExternalTable(externalTable, ifNotExists)
    }

  override def createDataBase(database: String, ifNotExists: Boolean): String =
    CREATE_DATABASE_PATTERN.format(
      serializeCreateDatabaseDDL(ifNotExists),
      database
    )

  override def dropExternalTable(externalTable: ExternalTable, ifExists: Boolean): String =
    DROP_TABLE_PATTERN.format(
      serializeDropTableDDL(ifExists),
      serializeName(externalTable)
    )

  override def createView(impalaView: ImpalaView, ifNotExists: Boolean): String =
    CREATE_VIEW_PATTERN.format(
      serializeCreateViewDDL(ifNotExists),
      serializeName(impalaView),
      serializeSelectFromTableDDL(impalaView)
    )

  override def dropView(impalaView: ImpalaView, ifExists: Boolean): String =
    DROP_VIEW_PATTERN.format(
      serializeDropViewDDL(ifExists),
      serializeName(impalaView)
    )

  private def createDefaultExternalTable(externalTable: ExternalTable, ifNotExists: Boolean) =
    CREATE_TABLE_PATTERN.format(
      serializeCreateTableDDL(ifNotExists),
      serializeName(externalTable),
      serializeSchema(externalTable),
      serializeTableProperties(externalTable),
      serializeDefaultTblKeyValueProperties()
    )

  private def createPartitionedExternalTable(
      externalTable: ExternalTable,
      ifNotExists: Boolean
  ) =
    CREATE_PARTITIONED_TABLE_PATTERN.format(
      serializeCreateTableDDL(ifNotExists),
      serializeName(externalTable),
      serializeSchema(externalTable),
      serializePartitions(externalTable),
      serializeTableProperties(externalTable),
      serializeDefaultTblKeyValueProperties()
    )

  private def serializeCreateTableDDL(ifNotExists: Boolean): String =
    if (ifNotExists) asString(" ", CREATE_EXTERNAL_TABLE_KEY, IF_NOT_EXISTS_KEY)
    else CREATE_EXTERNAL_TABLE_KEY

  private def serializeDropTableDDL(ifExists: Boolean): String =
    if (ifExists) asString(" ", DROP_TABLE_KEY, IF_EXISTS_KEY)
    else DROP_TABLE_KEY

  private def serializeCreateDatabaseDDL(ifNotExists: Boolean): String =
    if (ifNotExists) asString(" ", CREATE_DATABASE_KEY, IF_NOT_EXISTS_KEY)
    else CREATE_DATABASE_KEY

  private def serializeName(impalaEntity: ImpalaEntity): String =
    asString(".", impalaEntity.database, impalaEntity.name)

  private def serializeSchema(impalaEntity: ImpalaEntity): String =
    asString(
      ",",
      impalaEntity.schema.map {
        case Field(n, t, Some(d)) => asString(" ", n, t.show, s"COMMENT '$d'")
        case Field(n, t, None)    => asString(" ", n, t.show)
      }: _*
    )

  private def serializePartitions(externalTable: ExternalTable): String =
    asString(",", externalTable.partitions.map(e => asString(" ", e.name, e.`type`.show)).toSeq: _*)

  private def serializeTableProperties(externalTable: ExternalTable): String =
    externalTable.format match {
      case Csv     => serializeCsvTableProperties(externalTable)
      case Parquet => serializeParquetTableProperties(externalTable)
    }

  private def serializeCsvTableProperties(externalTable: ExternalTable) = {
    val rowFormat = ROW_FORMAT_PATTERN.format("DELIMITED FIELDS TERMINATED BY ','")
    val storedAs = STORED_AS_PATTERN.format("TEXTFILE")
    val location = LOCATION_PATTERN.format(externalTable.location)
    asString(" ", rowFormat, storedAs, location)
  }

  private def serializeParquetTableProperties(externalTable: ExternalTable): String = {
    val storedAs = STORED_AS_PATTERN.format("PARQUET")
    val location = LOCATION_PATTERN.format(externalTable.location)
    asString(" ", storedAs, location)
  }

  private def serializeDefaultTblKeyValueProperties(): String =
    defaultTblProperties
      .map(p => s"'${p._1}'='${p._2}'")
      .mkString(s" $TBLPROPERTIES_KEY (", ", ", ")")

  def serializeCreateViewDDL(ifNotExists: Boolean): String =
    if (ifNotExists) asString(" ", CREATE_VIEW_KEY, IF_NOT_EXISTS_KEY)
    else CREATE_VIEW_KEY

  private def serializeDropViewDDL(ifExists: Boolean): String =
    if (ifExists) asString(" ", DROP_VIEW_KEY, IF_EXISTS_KEY)
    else DROP_VIEW_KEY

  private def serializeSelectFromTableDDL(impalaView: ImpalaView): String =
    asString(
      " ",
      SELECT_PATTERN.format(
        asString(",", impalaView.schema.map(_.name): _*)
      ),
      FROM_PATTERN.format(asString(".", impalaView.database, impalaView.readsFromTableName))
    )

  private def asString(separator: String, values: String*): String =
    values.mkString(separator)

}
