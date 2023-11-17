package it.agilelab.provisioning.impala.table.provisioner.core.model

import it.agilelab.provisioning.commons.support.ParserSupport
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaFormat.{Csv, Parquet}
import org.scalatest.funsuite.AnyFunSuite
import cats.implicits._

class ImpalaFormatTest extends AnyFunSuite with ParserSupport {
  Seq(
    ("\"CSV\"", Csv),
    ("\"PARQUET\"", Parquet)
  ) foreach { case (format: String, impalaFormat: ImpalaFormat) =>
    test(s"toJson with $impalaFormat return $format") {
      assert(toJson(impalaFormat) == format)
    }

    test(s"fromJson with $format return $impalaFormat") {
      assert(fromJson[ImpalaFormat](format) == Right(impalaFormat))
    }

  }

  Seq(
    ("\"csv\"", Csv),
    ("\"parquet\"", Parquet)
  ) foreach { case (format: String, impalaFormat: ImpalaFormat) =>
    test(s"fromJson with $format return $impalaFormat") {
      assert(fromJson[ImpalaFormat](format) == Right(impalaFormat))
    }

  }

  Seq(
    ("CSV", Csv),
    ("PARQUET", Parquet)
  ) foreach { case (format: String, impalaFormat: ImpalaFormat) =>
    test(s"show with $impalaFormat return $format") {
      assert(impalaFormat.show == format)
    }

  }



}
