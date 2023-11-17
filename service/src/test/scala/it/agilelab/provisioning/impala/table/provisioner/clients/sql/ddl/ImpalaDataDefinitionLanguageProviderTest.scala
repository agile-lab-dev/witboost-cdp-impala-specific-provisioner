package it.agilelab.provisioning.impala.table.provisioner.clients.sql.ddl

import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaFormat.{ Csv, Parquet }
import it.agilelab.provisioning.impala.table.provisioner.core.model.{
  ExternalTable,
  Field,
  ImpalaDataType
}
import org.scalatest.funsuite.AnyFunSuite

class ImpalaDataDefinitionLanguageProviderTest extends AnyFunSuite {

  val impalaDataDefinitionLanguageProvider = new ImpalaDataDefinitionLanguageProvider()

  test("createExternalTable on CSV ifNotExists=true") {
    val actual = impalaDataDefinitionLanguageProvider.createExternalTable(
      ExternalTable(
        "test_db",
        "test_table",
        Seq(
          Field("id", ImpalaDataType.ImpalaInt, Some("x")),
          Field("zzz", ImpalaDataType.ImpalaString, None),
          Field("aaa", ImpalaDataType.ImpalaInt, None)),
        Seq.empty,
        "location",
        Csv
      ),
      ifNotExists = true
    )
    val expected = "CREATE EXTERNAL TABLE IF NOT EXISTS test_db.test_table " +
      "(id INT COMMENT 'x',zzz STRING,aaa INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' " +
      "STORED AS TEXTFILE LOCATION 'location' TBLPROPERTIES ('impala.disableHmsSync'='false')"

    assert(expected == actual)
  }

  test("createExternalTable partitioned on CSV ifNotExists=true") {
    val actual = impalaDataDefinitionLanguageProvider.createExternalTable(
      ExternalTable(
        "test_db",
        "test_table",
        Seq(Field("id", ImpalaDataType.ImpalaInt, None)),
        Seq(Field("p1", ImpalaDataType.ImpalaString, None)),
        "location",
        Csv
      ),
      ifNotExists = true
    )
    val expected = "CREATE EXTERNAL TABLE IF NOT EXISTS test_db.test_table " +
      "(id INT) PARTITIONED BY (p1 STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' " +
      "STORED AS TEXTFILE LOCATION 'location' TBLPROPERTIES ('impala.disableHmsSync'='false')"

    assert(expected == actual)
  }

  test("createExternalTable on CSV ifNotExists=false") {
    val actual = impalaDataDefinitionLanguageProvider.createExternalTable(
      ExternalTable(
        "test_db",
        "test_table",
        Seq(Field("id", ImpalaDataType.ImpalaInt, None)),
        Seq.empty,
        "location",
        Csv
      ),
      ifNotExists = false
    )
    val expected = "CREATE EXTERNAL TABLE test_db.test_table " +
      "(id INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' " +
      "STORED AS TEXTFILE LOCATION 'location' TBLPROPERTIES ('impala.disableHmsSync'='false')"

    assert(expected == actual)
  }

  test("createExternalTable on PARQUET ifNotExists=true") {
    val actual = impalaDataDefinitionLanguageProvider.createExternalTable(
      ExternalTable(
        "test_db",
        "test_table",
        Seq(
          Field("id", ImpalaDataType.ImpalaInt, None),
          Field("desc", ImpalaDataType.ImpalaInt, None)),
        Seq.empty,
        "location",
        Parquet
      ),
      ifNotExists = true
    )
    val expected = "CREATE EXTERNAL TABLE IF NOT EXISTS test_db.test_table " +
      "(id INT,desc INT) STORED AS PARQUET LOCATION 'location' TBLPROPERTIES ('impala.disableHmsSync'='false')"

    assert(expected == actual)
  }

  test("createExternalTable partitioned on PARQUET ifNotExists=true") {
    val actual = impalaDataDefinitionLanguageProvider.createExternalTable(
      ExternalTable(
        "test_db",
        "test_table",
        Seq(Field("id", ImpalaDataType.ImpalaInt, None)),
        Seq(Field("p1", ImpalaDataType.ImpalaString, None)),
        "location",
        Parquet
      ),
      ifNotExists = true
    )
    val expected = "CREATE EXTERNAL TABLE IF NOT EXISTS test_db.test_table " +
      "(id INT) PARTITIONED BY (p1 STRING) " +
      "STORED AS PARQUET LOCATION 'location' TBLPROPERTIES ('impala.disableHmsSync'='false')"

    assert(expected == actual)
  }

  test("createDataBase ifNotExists=true") {
    val actual = impalaDataDefinitionLanguageProvider.createDataBase(
      "xyz",
      ifNotExists = true
    )
    val expected = "CREATE DATABASE IF NOT EXISTS xyz"

    assert(actual == expected)
  }
}
