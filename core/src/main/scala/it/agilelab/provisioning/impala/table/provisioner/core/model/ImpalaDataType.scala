package it.agilelab.provisioning.impala.table.provisioner.core.model

import cats.Show
import cats.implicits._
import cats.data.NonEmptyList

sealed trait ImpalaDataType extends Product with Serializable
sealed trait ImpalaPrimitiveDataType extends ImpalaDataType
sealed trait ImpalaComplexDataType extends ImpalaDataType

object ImpalaDataType {
  //primitive types
  case object ImpalaBigInt extends ImpalaPrimitiveDataType
  case object ImpalaBoolean extends ImpalaPrimitiveDataType
  final case class ImpalaChar(length: Int) extends ImpalaPrimitiveDataType
  case object ImpalaDate extends ImpalaPrimitiveDataType
  final case class ImpalaDecimal(precision: Int = 9, scale: Int = 0) extends ImpalaPrimitiveDataType
  case object ImpalaDouble extends ImpalaPrimitiveDataType
  case object ImpalaFloat extends ImpalaPrimitiveDataType
  case object ImpalaInt extends ImpalaPrimitiveDataType
  case object ImpalaSmallInt extends ImpalaPrimitiveDataType
  case object ImpalaString extends ImpalaPrimitiveDataType
  case object ImpalaTimestamp extends ImpalaPrimitiveDataType
  case object ImpalaTinyInt extends ImpalaPrimitiveDataType
  final case class ImpalaVarChar(max_length: Int) extends ImpalaPrimitiveDataType

  //complex types
  final case class ImpalaArray(`type`: ImpalaDataType) extends ImpalaComplexDataType
  final case class ImpalaMap(`type`: (ImpalaPrimitiveDataType, ImpalaDataType))
      extends ImpalaComplexDataType
  final case class ImpalaStruct(nonEmptyList: NonEmptyList[StructType])
      extends ImpalaComplexDataType

  final case class StructType(name: String, `type`: ImpalaDataType, comment: Option[String])

  implicit val showImpalaDataType: Show[ImpalaDataType] = Show.show {
    case ImpalaBigInt                    => show"BIGINT"
    case ImpalaBoolean                   => show"BOOLEAN"
    case ImpalaChar(length)              => show"CHAR($length)"
    case ImpalaDate                      => show"DATE"
    case ImpalaDecimal(precision, scale) => show"DECIMAL($precision,$scale)"
    case ImpalaDouble                    => show"DOUBLE"
    case ImpalaFloat                     => show"FLOAT"
    case ImpalaInt                       => show"INT"
    case ImpalaSmallInt                  => show"SMALLINT"
    case ImpalaString                    => show"STRING"
    case ImpalaTimestamp                 => show"TIMESTAMP"
    case ImpalaTinyInt                   => show"TINYINT"
    case ImpalaVarChar(max_length)       => show"VARCHAR($max_length)"

    case ImpalaArray(t) => show"ARRAY<${showImpalaDataType.show(t)}>"
    case ImpalaMap(t) =>
      show"MAP<${showImpalaDataType.show(t._1)},${showImpalaDataType.show(t._2)}>"
    case ImpalaStruct(l) => show"STRUCT<${l.map(_.show).mkString_(",")}>"
  }

  implicit val showStructType: Show[StructType] = Show.show(i =>
    i.comment
      .map(c => show"${i.name}: ${i.`type`} COMMENT '$c'")
      .getOrElse(show"${i.name}: ${i.`type`}"))

  object ImpalaChar {
    private def checkLength(length: Int): Option[Int] =
      if (length > 0 && length <= 255) Some(length)
      else None

    def create(length: Int): Option[ImpalaChar] =
      checkLength(length).map(ImpalaChar.apply)
  }

  object ImpalaVarChar {
    private def checkLength(length: Int): Option[Int] =
      if (length > 0 && length <= 65535) Some(length)
      else None

    def create(length: Int): Option[ImpalaVarChar] =
      checkLength(length).map(ImpalaVarChar.apply)
  }

  object ImpalaDecimal {
    private def checkPrecision(precision: Int): Option[Int] =
      if (precision > 0 && precision <= 38) Some(precision)
      else None

    private def checkScale(scale: Int, precision: Int): Option[Int] =
      if (scale <= precision) Some(scale)
      else None

    def create(precision: Int, scale: Int): Option[ImpalaDecimal] =
      (checkPrecision(precision), checkScale(scale, precision)).mapN(ImpalaDecimal.apply)
  }
}
