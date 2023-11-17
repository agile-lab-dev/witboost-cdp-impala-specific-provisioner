package it.agilelab.provisioning.impala.table.provisioner.core.support

import cats.data.NonEmptyList
import cats.implicits._
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaDataType.{
  ImpalaArray,
  ImpalaBigInt,
  ImpalaBoolean,
  ImpalaChar,
  ImpalaDate,
  ImpalaDecimal,
  ImpalaDouble,
  ImpalaInt,
  ImpalaMap,
  ImpalaSmallInt,
  ImpalaString,
  ImpalaStruct,
  ImpalaTimestamp,
  ImpalaTinyInt,
  ImpalaVarChar,
  StructType
}
import it.agilelab.provisioning.impala.table.provisioner.core.model.{
  ImpalaDataType,
  ImpalaPrimitiveDataType
}
import it.agilelab.provisioning.mesh.self.service.api.model.openmetadata.{ Column, ColumnDataType }
import it.agilelab.provisioning.impala.table.provisioner.core.model.{
  ImpalaDataType,
  ImpalaPrimitiveDataType
}
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaDataType.{
  ImpalaArray,
  ImpalaBigInt,
  ImpalaBoolean,
  ImpalaChar,
  ImpalaDate,
  ImpalaDecimal,
  ImpalaDouble,
  ImpalaInt,
  ImpalaSmallInt,
  ImpalaString,
  ImpalaTimestamp,
  ImpalaTinyInt,
  ImpalaVarChar
}

import java.util.Locale
import scala.util.Try
import scala.util.matching.Regex

trait ImpalaDataTypeSupport {
  val regex: Regex = "decimal\\(([0-9]+),([0-9]+)\\)".r

  def fromOpenMetadataColumn(c: Column): Either[String, ImpalaDataType] =
    if (!isComplex(c.dataType)) fromPrimitive(c.dataType, c.dataLength, c.dataTypeDisplay)
    else
      c.dataType match {
        case ColumnDataType.ARRAY =>
          c.arrayDataType match {
            case Some(t) if !isComplex(t) =>
              for {
                subType <- fromPrimitive(t, c.dataLength, c.dataTypeDisplay)
              } yield ImpalaArray(subType)

            case Some(t) =>
              for {
                ch      <- c.children.toRight("children must be specified for complex arrayDataType")
                subType <- fromComplex(t, ch)
              } yield ImpalaArray(subType)

            case None => Left("arrayDataType must be specified for ARRAY data type")
          }

        case ColumnDataType.MAP | ColumnDataType.STRUCT =>
          for {
            ch    <- c.children.toRight(s"children must be specified for ${c.dataType} type")
            cType <- fromComplex(c.dataType, ch)
          } yield cType

        case t => Left(s"$t is not an allowed Impala Data type")
      }

  def canBeUsedAsPartitionColumn(instance: ImpalaDataType): Boolean = instance match {
    case ImpalaBoolean | ImpalaTimestamp | ImpalaArray(_) | ImpalaMap(_) | ImpalaStruct(_) => false
    case _                                                                                 => true
  }

  private def isComplex(dt: ColumnDataType) = dt match {
    case ColumnDataType.ARRAY | ColumnDataType.MAP | ColumnDataType.STRUCT => true
    case _                                                                 => false
  }

  private def fromComplex(
      dt: ColumnDataType,
      children: Seq[Column]
  ): Either[String, ImpalaDataType] = dt match {
    case ColumnDataType.ARRAY =>
      for {
        cols <- children.map(col => fromOpenMetadataColumn(col)).sequence
        dt   <- cols.headOption.toRight("children must have one column for ARRAY complex data type")
        dtArray <- Try(dt.asInstanceOf[ImpalaArray]).toEither.leftMap(_ =>
          "arrayDataType must match children data type")
      } yield dtArray

    case ColumnDataType.MAP =>
      for {
        cols  <- children.map(col => fromOpenMetadataColumn(col)).sequence
        dtKey <- cols.headOption.toRight("children must have first column for MAP key type")
        dtKeyPrimitive <- Try(dtKey.asInstanceOf[ImpalaPrimitiveDataType]).toEither.leftMap(_ =>
          "MAP key must be a primitive type")
        dtValue <- cols.lift(1).toRight("children must have second column for MAP value type")
      } yield ImpalaMap((dtKeyPrimitive, dtValue))

    case ColumnDataType.STRUCT =>
      for {
        dt <- children.map(c => fromOpenMetadataColumn(c)).sequence
        maybeStructs = children
          .zip(dt)
          .map(colDt => StructType(colDt._1.name, colDt._2, colDt._1.description))
        structs <- NonEmptyList
          .fromList(maybeStructs.toList)
          .toRight("STRUCT complex type must have at least one children")
      } yield ImpalaStruct(structs)

    case t => Left(s"$t is not a complex data type")
  }

  private def fromPrimitive(
      dataType: ColumnDataType,
      dataLength: Option[Int],
      dataTypeDisplay: Option[String]
  ): Either[String, ImpalaDataType] = dataType match {
    case ColumnDataType.TINYINT  => Right(ImpalaTinyInt)
    case ColumnDataType.SMALLINT => Right(ImpalaSmallInt)
    case ColumnDataType.INT      => Right(ImpalaInt)
    case ColumnDataType.BIGINT   => Right(ImpalaBigInt)
    case ColumnDataType.DOUBLE   => Right(ImpalaDouble)
    case ColumnDataType.DECIMAL =>
      extractDecimalConfig(dataTypeDisplay)
        .map(ps =>
          ImpalaDecimal
            .create(ps._1, ps._2)
            .toRight("DECIMAL precision must be between 1-38; scale must be <= precision"))
        .sequence
        .map(_.getOrElse(ImpalaDecimal()))
    case ColumnDataType.TIMESTAMP => Right(ImpalaTimestamp)
    case ColumnDataType.DATE      => Right(ImpalaDate)
    case ColumnDataType.STRING    => Right(ImpalaString)
    case ColumnDataType.CHAR =>
      dataLength
        .toRight[String]("dataLength must be specified for CHAR data type")
        .flatMap(dl => ImpalaChar.create(dl).toRight("CHAR length must be between 1 and 255"))
    case ColumnDataType.VARCHAR =>
      dataLength
        .toRight[String]("dataLength must be specified for VARCHAR data type")
        .flatMap(dl =>
          ImpalaVarChar.create(dl).toRight("VARCHAR length must be between 1 and 65535"))
    case ColumnDataType.BOOLEAN => Right(ImpalaBoolean)
    case t                      => Left(s"$t is not a supported Impala primitive data type")
  }

  private def extractDecimalConfig(dataTypeDisplay: Option[String]): Option[(Int, Int)] =
    dataTypeDisplay.map(s => s.toLowerCase(Locale.getDefault)).getOrElse("") match {
      case regex(precision, scale) =>
        Try(Some((precision.toInt, scale.toInt))).toEither.getOrElse(None)
      case _ => None
    }

}
