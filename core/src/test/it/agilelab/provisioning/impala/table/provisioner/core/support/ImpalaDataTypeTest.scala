package it.agilelab.provisioning.impala.table.provisioner.core.support

import cats.data.NonEmptyList
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaDataType
import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaDataType._
import org.scalatest.funsuite.AnyFunSuite
import cats.implicits._

class ImpalaDataTypeTest extends AnyFunSuite {
  Seq(
    (ImpalaBigInt, "BIGINT"),
    (ImpalaBoolean, "BOOLEAN"),
    (ImpalaChar(4), "CHAR(4)"),
    (ImpalaDate, "DATE"),
    (ImpalaDecimal(20, 7), "DECIMAL(20,7)"),
    (ImpalaDouble, "DOUBLE"),
    (ImpalaFloat, "FLOAT"),
    (ImpalaInt, "INT"),
    (ImpalaSmallInt, "SMALLINT"),
    (ImpalaString, "STRING"),
    (ImpalaTimestamp, "TIMESTAMP"),
    (ImpalaTinyInt, "TINYINT"),
    (ImpalaVarChar(32), "VARCHAR(32)"),
    (ImpalaArray(ImpalaInt), "ARRAY<INT>"),
    (ImpalaArray(ImpalaArray(ImpalaInt)), "ARRAY<ARRAY<INT>>"),
    (ImpalaArray(ImpalaMap(ImpalaInt,ImpalaInt)), "ARRAY<MAP<INT,INT>>"),
    (ImpalaArray(ImpalaStruct(NonEmptyList.fromListUnsafe(List(StructType("name", ImpalaInt, None))))), "ARRAY<STRUCT<name: INT>>"),
    (ImpalaMap(ImpalaInt,ImpalaInt), "MAP<INT,INT>"),
    (ImpalaMap(ImpalaInt,ImpalaArray(ImpalaInt)), "MAP<INT,ARRAY<INT>>"),
    (ImpalaMap(ImpalaInt,ImpalaMap((ImpalaInt,ImpalaInt))), "MAP<INT,MAP<INT,INT>>"),
    (ImpalaMap(ImpalaInt,ImpalaStruct(NonEmptyList.fromListUnsafe(List(StructType("name", ImpalaInt, None))))), "MAP<INT,STRUCT<name: INT>>"),
    (ImpalaStruct(NonEmptyList.fromListUnsafe(List(StructType("name", ImpalaInt, Some("comment"))))), "STRUCT<name: INT COMMENT 'comment'>"),
    (ImpalaStruct(NonEmptyList.fromListUnsafe(List(StructType("name", ImpalaInt, Some("comment")),StructType("name1", ImpalaArray(ImpalaString), Some("comment1"))))),
      "STRUCT<name: INT COMMENT 'comment',name1: ARRAY<STRING> COMMENT 'comment1'>"),
  ) foreach { case (idt: ImpalaDataType, s: String) =>

    test(s"show with $idt, return $s") {
      assert(show"$idt" == s)
    }
  }

}
