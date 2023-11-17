package it.agilelab.provisioning.impala.table.provisioner.core.model

import cats.Show
import io.circe.{ Decoder, Encoder }

sealed trait ImpalaFormat extends Product with Serializable

object ImpalaFormat {
  case object Parquet extends ImpalaFormat
  case object Csv extends ImpalaFormat

  implicit val showImpalaFormat: Show[ImpalaFormat] = Show.show {
    case Parquet => "PARQUET"
    case Csv     => "CSV"
  }

  implicit val impalaFormatEncoder: Encoder[ImpalaFormat] = Encoder[String].contramap {
    case Parquet => "PARQUET"
    case Csv     => "CSV"
  }

  implicit val impalaFormatDecoder: Decoder[ImpalaFormat] = Decoder[String].emap {
    case "PARQUET" | "parquet" => Right(Parquet)
    case "CSV" | "csv"         => Right(Csv)
    case other                 => Left(s"Invalid Impala format: $other")
  }
}
