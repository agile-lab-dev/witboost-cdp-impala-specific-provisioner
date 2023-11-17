package it.agilelab.provisioning.impala.table.provisioner.clients.sql.ddl

import cats.implicits._
import it.agilelab.provisioning.impala.table.provisioner.core.model.{ ExternalTable, Field }
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaFormat.{ Csv, Parquet }

import java.util.Locale

class ImpalaDataDefinitionLanguageProvider extends DataDefinitionLanguageProvider {

  private val CREATE_DATABASE_PATTERN = "%s %s"
  private val CREATE_TABLE_PATTERN = "%s %s (%s) %s%s"
  private val CREATE_PARTITIONED_TABLE_PATTERN = "%s %s (%s) PARTITIONED BY (%s) %s%s"

  private val CREATE_DATABASE_KEY = "CREATE DATABASE"
  private val CREATE_EXTERNAL_TABLE_KEY = "CREATE EXTERNAL TABLE"
  private val IF_NOT_EXISTS_KEY = "IF NOT EXISTS"
  private val TBLPROPERTIES_KEY = "TBLPROPERTIES"

  private val STORED_AS_PATTERN = "STORED AS %s"
  private val ROW_FORMAT_PATTERN = "ROW FORMAT %s"
  private val LOCATION_PATTERN = "LOCATION '%s'"

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

  private def createDefaultExternalTable(externalTable: ExternalTable, ifNotExists: Boolean) =
    CREATE_TABLE_PATTERN.format(
      serializeCreateTableDDL(ifNotExists),
      serializeTableName(externalTable),
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
      serializeTableName(externalTable),
      serializeSchema(externalTable),
      serializePartitions(externalTable),
      serializeTableProperties(externalTable),
      serializeDefaultTblKeyValueProperties()
    )

  private def serializeCreateTableDDL(ifNotExists: Boolean): String =
    if (ifNotExists) asString(" ", CREATE_EXTERNAL_TABLE_KEY, IF_NOT_EXISTS_KEY)
    else CREATE_EXTERNAL_TABLE_KEY

  private def serializeCreateDatabaseDDL(ifNotExists: Boolean): String =
    if (ifNotExists) asString(" ", CREATE_DATABASE_KEY, IF_NOT_EXISTS_KEY)
    else CREATE_DATABASE_KEY

  private def serializeTableName(externalTable: ExternalTable): String =
    asString(".", externalTable.database, externalTable.tableName)

  private def serializeSchema(externalTable: ExternalTable): String =
    asString(
      ",",
      externalTable.schema.map {
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

  private def asString(separator: String, values: String*): String =
    values.mkString(separator)

}
