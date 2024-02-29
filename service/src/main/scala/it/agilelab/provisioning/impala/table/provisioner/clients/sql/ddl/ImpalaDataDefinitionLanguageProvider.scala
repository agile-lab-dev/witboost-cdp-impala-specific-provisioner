package it.agilelab.provisioning.impala.table.provisioner.clients.sql.ddl

import cats.implicits._
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaFormat._
import it.agilelab.provisioning.impala.table.provisioner.core.model.{
  ExternalTable,
  Field,
  ImpalaEntity,
  ImpalaFormat,
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
  private val headerTblProperty: (String, String) = "skip.header.line.count" -> "1"

  private val HADOOP_DEFAULT_DELIMITER: Byte = 0x01

  override def createExternalTable(
      externalTable: ExternalTable,
      ifNotExists: Boolean
  ): String =
    externalTable match {
      case e if e.partitions.nonEmpty =>
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
      serializeTblKeyValueProperties(externalTable, addDefaultTblProperties = true)
    )

  private def createPartitionedExternalTable(
      externalTable: ExternalTable,
      ifNotExists: Boolean
  ) =
    CREATE_PARTITIONED_TABLE_PATTERN.format(
      serializeCreateTableDDL(ifNotExists), // CREATE TABLE [IF NOT EXISTS]
      serializeName(externalTable), // <DB>.<NAME>
      serializeSchema(externalTable), //  columns
      serializePartitions(externalTable), // PARTITIONED BY ( columns )
      serializeTableProperties(externalTable), // STORED AS, ROWS DELIMITED ...
      serializeTblKeyValueProperties(
        externalTable,
        addDefaultTblProperties = true
      ) // TBLPROPERTIES( ... )
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
    asString(",", externalTable.partitions.map(e => asString(" ", e.name, e.`type`.show)): _*)

  private def serializeTableProperties(externalTable: ExternalTable): String =
    externalTable.format match {
      case Csv | Textfile => serializeDelimitedFileTableProperties(externalTable)
      case Parquet | Avro => serializeBasicFileTableProperties(externalTable)
    }

  /** Creates the DDL statements for textfiles, converting the received delimiter to a format understandable by Impala.
    * If no delimiter is given, the Hadoop default one is used.
    * For Csv, a hardcoded "," delimiter is used, as we use Csv merely as a shorthand for comma-delimited TEXTFILEs.
    * @param externalTable External table with the format and delimiter information
    * @return DDL Statement [DELIMITED FIELDS TERMINATED BY '...'] STORED AS TEXTFILE
    */
  private def serializeDelimitedFileTableProperties(externalTable: ExternalTable): String = {
    val delimiter: Option[String] = externalTable.format match {
      case Parquet | Avro | Textfile => externalTable.delimiter.flatMap(byteToDelimiter)
      case Csv                       => Some(",")
    }

    val rowFormat =
      delimiter.fold("")(d => ROW_FORMAT_PATTERN.format(s"DELIMITED FIELDS TERMINATED BY '$d'"))
    val storedAs = STORED_AS_PATTERN.format(show"${Textfile}")
    val location = LOCATION_PATTERN.format(externalTable.location)
    asString(" ", rowFormat, storedAs, location)
  }

  private def serializeBasicFileTableProperties(externalTable: ExternalTable): String = {
    val storedAs = STORED_AS_PATTERN.format(externalTable.format.show)
    val location = LOCATION_PATTERN.format(externalTable.location)
    asString(" ", storedAs, location)
  }

  /** Retrieves the properties to be used, and then merge them with the user custom table properties.
    * On case of conflict between default table properties and user-defined table properties, user-defined properties take precedence
    *
    * @param externalTable External table defining he user table properties and whether the data files have a header
    * @param addDefaultTblProperties Whether to add the default table properties
    * @return DDL statement TBLPROPERTIES(...) as a String
    */
  def serializeTblKeyValueProperties(
      externalTable: ExternalTable,
      addDefaultTblProperties: Boolean
  ): String = {

    val properties: Seq[(String, String)] =
      (if (addDefaultTblProperties) defaultTblProperties else Seq.empty[(String, String)]) ++
        (
          if (externalTable.header && Seq(Textfile, Csv).contains(externalTable.format))
            Seq(headerTblProperty)
          else Seq.empty[(String, String)]
        )

    properties
      .foldLeft(externalTable.tblProperties) { (properties, p) =>
        properties.updatedWith(p._1) {
          case Some(value) => Some(value)
          case None        => Some(p._2)
        }
      }
      .map(p => s"'${p._1}'='${p._2}'")
      .mkString(s" $TBLPROPERTIES_KEY (", ", ", ")")
  }

  private def serializeCreateViewDDL(ifNotExists: Boolean): String =
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

  /** Parses a Byte to an Impala delimiter that it can understand.
    *
    *  - For printable characters, the byte is transformed to String (e.g. 0x2c -> ",")
    *  - For the Hadoop default delimiter, None is returned
    *  - For other non-printable characters,
    *   the byte is transformed to the String representation of the byte number in the range -127..128 (e.g. 0xFE -> "-2")
    * @param b Byte to transform
    * @return Some(String) for all characters excepting the [[HADOOP_DEFAULT_DELIMITER]], for which is returned None
    */
  private def byteToDelimiter(b: Byte): Option[String] =
    if (b >= 32 && b <= 126) { // Printable characters, so let's print them for human readability
      Some(b.toChar.toString)
    } else if (b != HADOOP_DEFAULT_DELIMITER) { // Non-printable characters excluding Ctrl-A
      Some(b.toInt.toString)
    } else
      None // Ctrl-A is None since we don't need to specify the row delimiter statement for default

  private def asString(separator: String, values: String*): String =
    values.mkString(separator)

}
