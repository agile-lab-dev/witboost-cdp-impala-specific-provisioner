package it.agilelab.provisioning.impala.table.provisioner.core.model

sealed trait ImpalaEntity {
  def database: String
  def name: String
  def schema: Seq[Field]
}

final case class ImpalaEntityImpl(
    override val database: String,
    override val name: String,
    override val schema: Seq[Field]
) extends ImpalaEntity

/** Class representing an External Table, a logical entity that defines a table schema representing data files
  * are typically produced outside Impala and queried from their original locations
  * @param database Database name
  * @param name Table name
  * @param schema Seq of [[Field]] representing the columns on the table schema
  * @param partitions Seq of [[Field]] with a subset of the schema representing the columns used to partition the table
  * @param location External location path or connector of the stored data files
  * @param format Data file format
  * @param delimiter For TEXTFILE format data files the character in the range -127..128 delimiting the fields in a row.
  *                  If set to None for TEXTFILE, the default Hadoop delimiter is used (ASCII 0x01)
  * @param tblProperties Map of string key-value pairs to add to the table TBLPROPERTIES
  * @param header Specifies for TEXTFILE format data files whether the first row of each file is the data header or not.
  */
final case class ExternalTable(
    override val database: String,
    override val name: String,
    override val schema: Seq[Field],
    partitions: Seq[Field],
    location: String,
    format: ImpalaFormat,
    delimiter: Option[Byte],
    tblProperties: Map[String, String],
    header: Boolean
) extends ImpalaEntity

/** Class representing a View, a logical entity built as a query on top of another table or view in the same database.
  *
  * @param database Database name
  * @param name View name
  * @param schema Seq of [[Field]] representing the columns to be queried from the underlying table/view
  * @param readsFromSource If present, information about the table or view from which this view queries data.
  *                        Ignored when a query statement for the source is given in querySourceStatement instead
  * @param querySourceStatement If present, SELECT statement to use as the source for the View
  */
final case class ImpalaView(
    override val database: String,
    override val name: String,
    override val schema: Seq[Field],
    readsFromSource: Option[ImpalaEntity],
    querySourceStatement: Option[String]
) extends ImpalaEntity
