package it.agilelab.provisioning.impala.table.provisioner.core.model

import cats.Show
import io.circe.{ Decoder, Encoder }

sealed trait ImpalaFormat extends Product with Serializable

object ImpalaFormat {
  case object Parquet extends ImpalaFormat
  case object Csv extends ImpalaFormat
  case object Textfile extends ImpalaFormat
  case object Avro extends ImpalaFormat

  // Used for DDL definition, keep as-is
  implicit val showImpalaFormat: Show[ImpalaFormat] = Show.show {
    case Parquet  => "PARQUET"
    case Csv      => "CSV"
    case Textfile => "TEXTFILE"
    case Avro     => "AVRO"
  }

  implicit val impalaFormatEncoder: Encoder[ImpalaFormat] = Encoder[String].contramap {
    case Parquet  => "PARQUET"
    case Csv      => "CSV"
    case Textfile => "TEXTFILE"
    case Avro     => "AVRO"
  }

  implicit val impalaFormatDecoder: Decoder[ImpalaFormat] = Decoder[String].emap {
    case parquet: String if parquet.equalsIgnoreCase("PARQUET")    => Right(Parquet)
    case csv: String if csv.equalsIgnoreCase("CSV")                => Right(Csv)
    case textfile: String if textfile.equalsIgnoreCase("TEXTFILE") => Right(Textfile)
    case avro: String if avro.equalsIgnoreCase("AVRO")             => Right(Avro)
    case other                                                     => Left(s"Invalid Impala format: $other")
  }
}
