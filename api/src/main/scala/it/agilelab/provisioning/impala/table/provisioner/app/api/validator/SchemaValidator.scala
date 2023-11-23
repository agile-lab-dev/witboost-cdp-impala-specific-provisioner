package it.agilelab.provisioning.impala.table.provisioner.app.api.validator

import cats.data.NonEmptyList
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import it.agilelab.provisioning.mesh.self.service.api.model.openmetadata.Column
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaDataType
import it.agilelab.provisioning.impala.table.provisioner.core.support.ImpalaDataTypeSupport

object SchemaValidator extends ImpalaDataTypeSupport with StrictLogging {

  def nonEmptySchema(schema: Seq[Column]): Boolean =
    schema.nonEmpty

  def isValidSchema(schema: Seq[Column]): Boolean = {
    val result = for {
      nonEmptyList <- NonEmptyList.fromList(schema.toList).toRight("Empty schema")
      _            <- nonEmptyList.map(fromOpenMetadataColumn).sequence
    } yield ()
    logger.info("Schema validation Result: {}", result)
    result.isRight
  }

  def validateColumnNames(schema: Seq[Column]): Boolean = {
    val isThereAnEmptyColumnName = schema.exists(c => c.name.isEmpty)
    val areThereDuplicatedNames = schema.map(c => c.name).distinct.length != schema.length
    !(isThereAnEmptyColumnName || areThereDuplicatedNames)
  }

  def arePartitionsValid(schema: Seq[Column], partitions: Option[Seq[String]]): Boolean = {
    val result = for {
      columns <- partitions
        .getOrElse(Seq.empty[String])
        .map(p => schema.find(c => c.name == p).toRight(s"Error. $p doesn't exists on schema"))
        .sequence
      impalaDataTypes <- columns.map(fromOpenMetadataColumn).sequence
      _               <- impalaDataTypes.map(isPartition).sequence
    } yield ()
    logger.info("Partition validation Result: {}", result)
    result.isRight
  }

  private def isPartition(dt: ImpalaDataType): Either[String, Unit] =
    if (canBeUsedAsPartitionColumn(dt)) Right()
    else Left(s"Error. Partitions cannot be performed on selected column because of its type")

}
