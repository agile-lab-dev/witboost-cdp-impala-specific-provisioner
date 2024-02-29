package it.agilelab.provisioning.impala.table.provisioner.gateway.mapper

import cats.implicits._
import it.agilelab.provisioning.impala.table.provisioner.core.model.{
  ExternalTable,
  Field,
  ImpalaTableCdw,
  TableParams
}
import it.agilelab.provisioning.impala.table.provisioner.core.support.ImpalaDataTypeSupport
import it.agilelab.provisioning.mesh.self.service.api.model.openmetadata.Column
import it.agilelab.provisioning.mesh.self.service.core.gateway.ComponentGatewayError

import scala.util.Try

object ExternalTableMapper extends ImpalaDataTypeSupport {
  def map(
      schema: Seq[Column],
      impalaSpecific: ImpalaTableCdw
  ): Either[ComponentGatewayError, ExternalTable] = for {
    tableSchema <- schema
      .filter(c => impalaSpecific.partitions.forall(seq => !seq.contains(c.name)))
      .map(toField)
      .sequence
    partitions <- schema
      .filter(c => impalaSpecific.partitions.exists(seq => seq.contains(c.name)))
      .map(toField)
      .sequence
    delimiter <- impalaSpecific.tableParams match {
      case Some(TableParams(_, Some(delimiter), _)) =>
        hexStringToByte(delimiter)
          .toRight(ComponentGatewayError(
            s"Failed to parse delimiter '$delimiter', is not a hex number nor a single ASCII character"))
          .map(Some(_))
      case _ => Right(None)
    }
  } yield ExternalTable(
    database = impalaSpecific.databaseName,
    name = impalaSpecific.tableName,
    schema = tableSchema,
    partitions = partitions,
    location = impalaSpecific.location,
    format = impalaSpecific.format,
    delimiter = delimiter,
    tblProperties = impalaSpecific.tableParams.fold(Map.empty[String, String])(_.tblProperties),
    header = impalaSpecific.tableParams.flatMap(_.header).getOrElse(false)
  )

  /** Transforms a string onto its byte representation iff it represents a hex number (0x2c, 2c, 2C...)
    * or if it consists of a single character whose byte value lies in the range of a single unsigned byte (0x0..0xff)
    * @param s String value to be converted to byte
    * @return Some(Byte) if string represents a hex value, or if it contains a single character in the byte range. None otherwise
    */
  def hexStringToByte(s: String): Option[Byte] = s match {
    case s"0x$hexNumber"                               => Try(Integer.parseInt(hexNumber, 16).toByte).toOption
    case s if s.length == 1 && s.charAt(0).toInt < 256 => Some(s.charAt(0).toByte)
    case s                                             => Try(Integer.parseInt(s, 16).toByte).toOption
  }

  private def toField(c: Column): Either[ComponentGatewayError, Field] = for {
    dt <- fromOpenMetadataColumn(c).leftMap(s => ComponentGatewayError(s))
  } yield Field(c.name, dt, c.description)

}
