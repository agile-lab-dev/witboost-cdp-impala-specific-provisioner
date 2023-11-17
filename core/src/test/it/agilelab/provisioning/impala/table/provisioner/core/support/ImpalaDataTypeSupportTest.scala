package it.agilelab.provisioning.impala.table.provisioner.core.support

import cats.data.NonEmptyList
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaDataType
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaDataType._
import it.agilelab.provisioning.mesh.self.service.api.model.openmetadata.{Column, ColumnConstraint, ColumnDataType}
import org.scalatest.funsuite.AnyFunSuite

class ImpalaDataTypeSupportTest extends AnyFunSuite with ImpalaDataTypeSupport {
  Seq(
    (getC("name", ColumnDataType.TINYINT), ImpalaTinyInt),
    (getC("name", ColumnDataType.SMALLINT), ImpalaSmallInt),
    (getC("name", ColumnDataType.INT), ImpalaInt),
    (getC("name", ColumnDataType.BIGINT), ImpalaBigInt),
    (getC("name", ColumnDataType.DOUBLE), ImpalaDouble),
    (getC("name", ColumnDataType.DECIMAL), ImpalaDecimal()),
    (getC("name", ColumnDataType.DECIMAL, dtd = Some("DECIMAL(10,2)")), ImpalaDecimal(10, 2)),
    (getC("name", ColumnDataType.TIMESTAMP), ImpalaTimestamp),
    (getC("name", ColumnDataType.DATE), ImpalaDate),
    (getC("name", ColumnDataType.STRING), ImpalaString),
    (getC("name", ColumnDataType.CHAR, None, dl = Some(4)), ImpalaChar(4)),
    (getC("name", ColumnDataType.VARCHAR, None, dl = Some(64)), ImpalaVarChar(64)),
    (getC("name", ColumnDataType.BOOLEAN), ImpalaBoolean),
    (getC("name", ColumnDataType.ARRAY, adt = Some(ColumnDataType.INT)), ImpalaArray(ImpalaInt)),
    (getC("name", ColumnDataType.ARRAY, adt = Some(ColumnDataType.DECIMAL), dtd = Some("DECIMAL(10,2)")), ImpalaArray(ImpalaDecimal(10, 2))),
    (getC("name", ColumnDataType.ARRAY, adt = Some(ColumnDataType.ARRAY),
      ch = Some(Seq(getC("", ColumnDataType.ARRAY, adt = Some(ColumnDataType.INT))))), ImpalaArray(ImpalaArray(ImpalaInt))),
    (getC("name", ColumnDataType.ARRAY, adt = Some(ColumnDataType.ARRAY),
      ch = Some(Seq(getC("", ColumnDataType.ARRAY, adt = Some(ColumnDataType.DECIMAL), dtd = Some("DECIMAL(10,2)"))))), ImpalaArray(ImpalaArray(ImpalaDecimal(10, 2)))),
    (getC("name", ColumnDataType.MAP,
      ch = Some(Seq(getC("key", ColumnDataType.INT), getC("value", ColumnDataType.INT)))), ImpalaMap((ImpalaInt,ImpalaInt))),
    (getC("name", ColumnDataType.MAP,
      ch = Some(Seq(getC("key", ColumnDataType.INT), getC("value", ColumnDataType.ARRAY, adt = Some(ColumnDataType.ARRAY),
        ch = Some(Seq(getC("", dt = ColumnDataType.ARRAY, adt = Some(ColumnDataType.INT)))))))),
      ImpalaMap((ImpalaInt,ImpalaArray(ImpalaArray(ImpalaInt))))),
    (getC("name", ColumnDataType.MAP,
      ch = Some(Seq(getC("key", ColumnDataType.INT), getC("value", ColumnDataType.STRUCT,
        ch = Some(Seq(getC("arr", dt = ColumnDataType.ARRAY, adt = Some(ColumnDataType.INT)))))))),
      ImpalaMap((ImpalaInt,ImpalaStruct(NonEmptyList.one(StructType("arr", ImpalaArray(ImpalaInt), None)))))),
    (getC("name", ColumnDataType.MAP,
      ch = Some(Seq(getC("key", ColumnDataType.INT), getC("value", ColumnDataType.MAP,
        ch = Some(Seq(getC("key", ColumnDataType.INT), getC("value", ColumnDataType.INT))))))),
      ImpalaMap((ImpalaInt,ImpalaMap((ImpalaInt, ImpalaInt))))),
    (getC("name", ColumnDataType.STRUCT,
      ch = Some(Seq(
        getC("name1", ColumnDataType.INT),
        getC("name2", ColumnDataType.DOUBLE),
        getC("name3", ColumnDataType.ARRAY, adt = Some(ColumnDataType.INT)),
        getC("name4", ColumnDataType.ARRAY, adt = Some(ColumnDataType.ARRAY), ch = Some(Seq(getC("arr", ColumnDataType.ARRAY, adt = Some(ColumnDataType.INT))))),
        getC("name5", ColumnDataType.MAP, ch = Some(Seq(getC("key", ColumnDataType.INT), getC("value", ColumnDataType.INT)))),
        getC("name6", ColumnDataType.STRUCT, ch = Some(Seq(getC("inner", ColumnDataType.INT))))
      ))),
      ImpalaStruct(NonEmptyList.fromListUnsafe(Seq(
        StructType("name1", ImpalaInt, None),
        StructType("name2", ImpalaDouble, None),
        StructType("name3", ImpalaArray(ImpalaInt), None),
        StructType("name4", ImpalaArray(ImpalaArray(ImpalaInt)), None),
        StructType("name5", ImpalaMap((ImpalaInt,ImpalaInt)), None),
        StructType("name6", ImpalaStruct(NonEmptyList.fromListUnsafe(Seq(StructType("inner", ImpalaInt, None)).toList)), None),
      ).toList)))
  ) foreach { case (c: Column, idt: ImpalaDataType) =>

    test(s"fromPrimitive with $c, return Right($idt)") {
      assert(fromOpenMetadataColumn(c) == Right(idt))
    }
  }

  Seq(
    (getC("name", ColumnDataType.CHAR), "dataLength must be specified for CHAR data type"),
    (getC("name", ColumnDataType.CHAR, dl = Some(5000)), "CHAR length must be between 1 and 255"),
    (getC("name", ColumnDataType.VARCHAR), "dataLength must be specified for VARCHAR data type"),
    (getC("name", ColumnDataType.VARCHAR, dl = Some(70000)), "VARCHAR length must be between 1 and 65535"),
    (getC("name", ColumnDataType.DECIMAL, dtd = Some("DECIMAL(40,0)")), "DECIMAL precision must be between 1-38; scale must be <= precision"),
    (getC("name", ColumnDataType.DECIMAL, dtd = Some("DECIMAL(15,39)")), "DECIMAL precision must be between 1-38; scale must be <= precision"),
    (getC("name", ColumnDataType.VARBINARY), "VARBINARY is not a supported Impala primitive data type"),
    (getC("name", ColumnDataType.ARRAY), "arrayDataType must be specified for ARRAY data type"),
    (getC("name", ColumnDataType.ARRAY, adt = Some(ColumnDataType.ARRAY),
      ch = Some(Seq(getC("name1", ColumnDataType.STRUCT, ch = Some(Seq(getC("name2", ColumnDataType.INT))))))), "arrayDataType must match children data type"),
    (getC("name", ColumnDataType.MAP), "children must be specified for MAP type"),
    (getC("name", ColumnDataType.MAP, ch=Some(Seq(getC("key", ColumnDataType.INT)))), "children must have second column for MAP value type"),
    (getC("name", ColumnDataType.MAP,
      ch = Some(Seq(getC("key", ColumnDataType.ARRAY, adt = Some(ColumnDataType.INT)), getC("value", ColumnDataType.INT)))), "MAP key must be a primitive type"),
    (getC("name", ColumnDataType.STRUCT, ch = Some(Seq.empty)), "STRUCT complex type must have at least one children")
  ) foreach { case (c: Column, error: String) =>

    test(s"fromPrimitive with $c return Left($error)") {
      assert(fromOpenMetadataColumn(c) == Left(error))
    }
  }


  private def getC(
    name: String,
    dt: ColumnDataType,
    adt: Option[ColumnDataType] = None,
    dl: Option[Int] = None,
    dtd: Option[String] = None,
    desc: Option[String] = None,
    fqdn: Option[String] = None,
    tags: Option[Seq[String]] = None,
    constraint: Option[ColumnConstraint] = None,
    op: Option[Int] = None,
    js: Option[String] = None,
    ch: Option[Seq[Column]] = None
  ) = Column(name, dt, adt, dl, dtd, desc, fqdn, tags, constraint, op, js, ch)

}
