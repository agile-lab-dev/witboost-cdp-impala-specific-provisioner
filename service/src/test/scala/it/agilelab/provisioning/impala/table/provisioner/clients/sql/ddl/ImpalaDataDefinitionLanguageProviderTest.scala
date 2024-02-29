package it.agilelab.provisioning.impala.table.provisioner.clients.sql.ddl

import it.agilelab.provisioning.impala.table.provisioner.core.model.ImpalaFormat.{
  Avro,
  Csv,
  Parquet,
  Textfile
}
import it.agilelab.provisioning.impala.table.provisioner.core.model.{
  ExternalTable,
  Field,
  ImpalaDataType,
  ImpalaView
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
        Csv,
        None,
        Map.empty,
        header = false
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
        Csv,
        None,
        Map.empty,
        header = false
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
        Csv,
        None,
        Map.empty,
        header = false
      ),
      ifNotExists = false
    )
    val expected = "CREATE EXTERNAL TABLE test_db.test_table " +
      "(id INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' " +
      "STORED AS TEXTFILE LOCATION 'location' TBLPROPERTIES ('impala.disableHmsSync'='false')"

    assert(expected == actual)
  }

  test("createExternalTable on CSV if header=true and extra tblProperties") {
    val actual = impalaDataDefinitionLanguageProvider.createExternalTable(
      ExternalTable(
        "test_db",
        "test_table",
        Seq(Field("id", ImpalaDataType.ImpalaInt, None)),
        Seq.empty,
        "location",
        Csv,
        None,
        Map("extra.property" -> "value", "impala.disableHmsSync" -> "true"),
        header = true
      ),
      ifNotExists = false
    )
    val expected = "CREATE EXTERNAL TABLE test_db.test_table " +
      "(id INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' " +
      "STORED AS TEXTFILE LOCATION 'location' " +
      "TBLPROPERTIES ('extra.property'='value', 'impala.disableHmsSync'='true', 'skip.header.line.count'='1')"

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
        Parquet,
        None,
        Map.empty,
        header = false
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
        Parquet,
        None,
        Map.empty,
        header = false
      ),
      ifNotExists = true
    )
    val expected = "CREATE EXTERNAL TABLE IF NOT EXISTS test_db.test_table " +
      "(id INT) PARTITIONED BY (p1 STRING) " +
      "STORED AS PARQUET LOCATION 'location' TBLPROPERTIES ('impala.disableHmsSync'='false')"

    assert(expected == actual)
  }

  test("createExternalTable on TEXTFILE without delim, if header=true and extra tblProperties") {
    val actual = impalaDataDefinitionLanguageProvider.createExternalTable(
      ExternalTable(
        "test_db",
        "test_table",
        Seq(Field("id", ImpalaDataType.ImpalaInt, None)),
        Seq.empty,
        "location",
        Textfile,
        None,
        Map("extra.property" -> "value", "impala.disableHmsSync" -> "true"),
        header = true
      ),
      ifNotExists = false
    )
    val expected = "CREATE EXTERNAL TABLE test_db.test_table " +
      "(id INT)  " +
      "STORED AS TEXTFILE LOCATION 'location' " +
      "TBLPROPERTIES ('extra.property'='value', 'impala.disableHmsSync'='true', 'skip.header.line.count'='1')"

    assert(expected == actual)
  }

  test("createExternalTable on TEXTFILE without delim, if header=false and extra tblProperties") {
    val actual = impalaDataDefinitionLanguageProvider.createExternalTable(
      ExternalTable(
        "test_db",
        "test_table",
        Seq(Field("id", ImpalaDataType.ImpalaInt, None)),
        Seq.empty,
        "location",
        Textfile,
        None,
        Map("extra.property" -> "value", "impala.disableHmsSync" -> "true"),
        header = false
      ),
      ifNotExists = false
    )
    val expected = "CREATE EXTERNAL TABLE test_db.test_table " +
      "(id INT)  " +
      "STORED AS TEXTFILE LOCATION 'location' " +
      "TBLPROPERTIES ('extra.property'='value', 'impala.disableHmsSync'='true')"

    assert(expected == actual)
  }

  test(
    "createExternalTable on TEXTFILE with printable delim, if header=true and extra tblProperties") {
    val actual = impalaDataDefinitionLanguageProvider.createExternalTable(
      ExternalTable(
        "test_db",
        "test_table",
        Seq(Field("id", ImpalaDataType.ImpalaInt, None)),
        Seq.empty,
        "location",
        Textfile,
        Some('|'.toByte),
        Map("extra.property" -> "value", "impala.disableHmsSync" -> "true"),
        header = true
      ),
      ifNotExists = false
    )
    val expected = "CREATE EXTERNAL TABLE test_db.test_table " +
      "(id INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' " +
      "STORED AS TEXTFILE LOCATION 'location' " +
      "TBLPROPERTIES ('extra.property'='value', 'impala.disableHmsSync'='true', 'skip.header.line.count'='1')"

    assert(expected == actual)
  }

  test(
    "createExternalTable on TEXTFILE with non-printable delim, if header=true and extra tblProperties") {
    val actual = impalaDataDefinitionLanguageProvider.createExternalTable(
      ExternalTable(
        "test_db",
        "test_table",
        Seq(Field("id", ImpalaDataType.ImpalaInt, None)),
        Seq.empty,
        "location",
        Textfile,
        Some(0xfe.toByte),
        Map("extra.property" -> "value", "impala.disableHmsSync" -> "true"),
        header = true
      ),
      ifNotExists = false
    )
    val expected = "CREATE EXTERNAL TABLE test_db.test_table " +
      "(id INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '-2' " +
      "STORED AS TEXTFILE LOCATION 'location' " +
      "TBLPROPERTIES ('extra.property'='value', 'impala.disableHmsSync'='true', 'skip.header.line.count'='1')"

    assert(expected == actual)
  }

  test("createExternalTable on AVRO ifNotExists=true") {
    val actual = impalaDataDefinitionLanguageProvider.createExternalTable(
      ExternalTable(
        "test_db",
        "test_table",
        Seq(
          Field("id", ImpalaDataType.ImpalaInt, None),
          Field("desc", ImpalaDataType.ImpalaInt, None)),
        Seq.empty,
        "location",
        Avro,
        None,
        Map.empty,
        header = false
      ),
      ifNotExists = true
    )
    val expected = "CREATE EXTERNAL TABLE IF NOT EXISTS test_db.test_table " +
      "(id INT,desc INT) STORED AS AVRO LOCATION 'location' TBLPROPERTIES ('impala.disableHmsSync'='false')"

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

  test("create view ifNotExists=true") {
    val view = ImpalaView(
      database = "database",
      name = "viewName",
      schema = Seq(
        Field("id", ImpalaDataType.ImpalaInt, None),
        Field("name", ImpalaDataType.ImpalaInt, None)),
      readsFromTableName = "originalTableName"
    )

    val actual = impalaDataDefinitionLanguageProvider.createView(view, ifNotExists = true)
    val expected =
      "CREATE VIEW IF NOT EXISTS database.viewName AS SELECT id,name FROM database.originalTableName"

    assert(actual == expected)
  }

  test("create view ifNotExists=false") {
    val view = ImpalaView(
      database = "database",
      name = "viewName",
      schema = Seq(
        Field("id", ImpalaDataType.ImpalaInt, None),
        Field("name", ImpalaDataType.ImpalaInt, None)),
      readsFromTableName = "originalTableName"
    )

    val actual = impalaDataDefinitionLanguageProvider.createView(view, ifNotExists = false)
    val expected =
      "CREATE VIEW database.viewName AS SELECT id,name FROM database.originalTableName"

    assert(actual == expected)
  }

  test("drop view ifExists=true") {
    val view = ImpalaView(
      database = "database",
      name = "viewName",
      schema = Seq(
        Field("id", ImpalaDataType.ImpalaInt, None),
        Field("name", ImpalaDataType.ImpalaInt, None)),
      readsFromTableName = "originalTableName"
    )

    val actual = impalaDataDefinitionLanguageProvider.dropView(view, ifExists = true)
    val expected = "DROP VIEW IF EXISTS database.viewName"

    assert(actual == expected)
  }

  test("drop view ifExists=false") {
    val view = ImpalaView(
      database = "database",
      name = "viewName",
      schema = Seq(
        Field("id", ImpalaDataType.ImpalaInt, None),
        Field("name", ImpalaDataType.ImpalaInt, None)),
      readsFromTableName = "originalTableName"
    )

    val actual = impalaDataDefinitionLanguageProvider.dropView(view, ifExists = false)
    val expected = "DROP VIEW database.viewName"

    assert(actual == expected)
  }
}
