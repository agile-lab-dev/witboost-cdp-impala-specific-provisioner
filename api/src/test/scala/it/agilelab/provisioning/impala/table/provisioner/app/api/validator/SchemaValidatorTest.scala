package it.agilelab.provisioning.impala.table.provisioner.app.api.validator

import it.agilelab.provisioning.mesh.self.service.api.model.openmetadata.TagLabelType.Manual
import it.agilelab.provisioning.mesh.self.service.api.model.openmetadata.TagState.Confirmed
import it.agilelab.provisioning.mesh.self.service.api.model.openmetadata.{
  Column,
  ColumnConstraint,
  ColumnDataType,
  Tag
}
import org.scalatest.funsuite.AnyFunSuite

class SchemaValidatorTest extends AnyFunSuite {
  test("nonEmptySchema return true when not empty") {
    assert(SchemaValidator.nonEmptySchema(Seq(getC("column", ColumnDataType.INT))))
  }

  test("nonEmptySchema return false when empty") {
    assert(!SchemaValidator.nonEmptySchema(Seq.empty[Column]))
  }

  test("isValidSchema return false when empty") {
    assert(!SchemaValidator.isValidSchema(Seq.empty[Column]))
  }

  test("isValidSchema return false when schema invalid") {
    assert(
      !SchemaValidator.isValidSchema(
        Seq(
          getC("invalidChar", ColumnDataType.CHAR) //char needs dataLength defined..
        )))
  }

  test("isValidSchema return true") {
    assert(
      SchemaValidator.isValidSchema(
        Seq(
          getC("invalidChar", ColumnDataType.CHAR, dl = Some(4))
        )))
  }

  test("validateColumnNames return false when empty column name") {
    assert(
      !SchemaValidator.validateColumnNames(
        Seq(
          getC("", ColumnDataType.INT),
          getC("validINT", ColumnDataType.INT)
        )))
  }

  test("validateColumnNames return false when duplicated column names") {
    assert(
      !SchemaValidator.validateColumnNames(
        Seq(
          getC("validINT", ColumnDataType.INT),
          getC("validINT", ColumnDataType.INT)
        )))
  }

  test("validateColumnNames return true") {
    assert(
      SchemaValidator.validateColumnNames(
        Seq(
          getC(
            "validINT1",
            ColumnDataType.INT,
            tags = Some(Seq(Tag("PII", None, "Glossary", Manual, Confirmed, None)))),
          getC("validINT2", ColumnDataType.INT)
        )))
  }

  test("arePartitionsValid return false when partition fields do not exist") {
    assert(
      !SchemaValidator.arePartitionsValid(
        Seq(
          getC("column1", ColumnDataType.INT),
          getC("column2", ColumnDataType.INT)
        ),
        Some(Seq("column3"))
      ))
  }

  test("arePartitionsValid return false when partition field cannot be used as partition column") {
    assert(
      !SchemaValidator.arePartitionsValid(
        Seq(
          getC("column1", ColumnDataType.INT),
          getC("column2", ColumnDataType.BOOLEAN)
        ),
        Some(Seq("column2"))
      ))
  }

  test("arePartitionsValid return true") {
    assert(
      SchemaValidator.arePartitionsValid(
        Seq(
          getC("column1", ColumnDataType.INT),
          getC("column2", ColumnDataType.INT)
        ),
        Some(Seq("column2"))
      ))
  }

  test("arePartitionsValid return true when partitions is None") {
    assert(
      SchemaValidator.arePartitionsValid(
        Seq(
          getC("column1", ColumnDataType.INT),
          getC("column2", ColumnDataType.INT)
        ),
        None
      ))
  }

  test("arePartitionsValid return true when partitions are emtpy") {
    assert(
      SchemaValidator.arePartitionsValid(
        Seq(
          getC("column1", ColumnDataType.INT),
          getC("column2", ColumnDataType.INT)
        ),
        Some(Seq.empty[String])
      ))
  }

  private def getC(
      name: String,
      dt: ColumnDataType,
      adt: Option[ColumnDataType] = None,
      dl: Option[Int] = None,
      dtd: Option[String] = None,
      desc: Option[String] = None,
      fqdn: Option[String] = None,
      tags: Option[Seq[Tag]] = None,
      constraint: Option[ColumnConstraint] = None,
      op: Option[Int] = None,
      js: Option[String] = None,
      ch: Option[Seq[Column]] = None
  ) = Column(name, dt, adt, dl, dtd, desc, fqdn, tags, constraint, op, js, ch)
}
