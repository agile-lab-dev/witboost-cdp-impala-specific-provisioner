# Impala Component Descriptor

The Impala provisioner uses the `specific` field for receiving the information necessary to deploy the Impala components. For Output Ports, it also uses the `dataContract.schema` to define the table/view schema.

| Field                      | Name                 | Description                                                                                                                                                                                                                                                                                                                                                                                                            | Required                                                                             |
|----------------------------|----------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------|
| `dataContract.schema`      | Data contract schema | Only for Output Ports. OpenMetadata Column schema defining the schema of the table or view to be created.                                                                                                                                                                                                                                                                                                              | Yes                                                                                  |
| `specific.databaseName`    | Database name        | Database to be created to handle the component tables or views. We recommend the Impala Database name to be equal to `$Domain_$DataProductName_$MajorVersion` and contain only the characters `[a-zA-Z0-9_]`. All other characters (like spaces or dashes) must be replaced with underscores (`_`). This allows to manage a single database for each data product, allowing for fine-grained authorization mechanisms. | Yes                                                                                  |
| `specific.tableName`       | Table name           | Table name to be created, or when provisioning a view, the name of the table exposed by the view. We recommend Impala Table name to be equal to `$Domain_$DPName_$DPMajorVrs_$ComponentName` and contain only the characters `[a-zA-Z0-9_]`. All other characters (like spaces or dashes) must be replaced with underscores (`_`).                                                                                     | Yes                                                                                  |                                                                                      
| `specific.format`          | Data file Format     | Format of the data files an external table exposes. Only required for table creation. Supported values are `CSV`, `PARQUET`, `TEXTFILE` and `AVRO` formats.                                                                                                                                                                                                                                                            | Only for External Tables                                                             |
| `specific.location`        | Location             | Location in S3 (CDP Public) or HDFS (CDP Private) where the data files are located                                                                                                                                                                                                                                                                                                                                     | Only for External Tables                                                             |
| `specific.partitions`      | Partitions           | List of columns used to partition the data.                                                                                                                                                                                                                                                                                                                                                                            | Only for External Tables. If the table doesn't have partitions, `[]`should be passed |                                                                                                                      
| `specific.viewName`        | View name            | Supported only for CDP Private Cloud. Sent when provisioning a view to define its name. We recommend the same naming conventions as for table names.                                                                                                                                                                                                                                                                   | If present, a view will be provisioned                                               |                                                                                                                                                                                                                                                                                                            
| `specific.tableParameters` | Table parameters     | Extra table parameters to define TBLPROPERTIES, text file delimiter and header, etc.                                                                                                                                                                                                                                                                                                                                   | No                                                                                   |                                                                                                                                                                                                                                                                                                                                                                                

## Index 
 
- [Output Ports](#output-ports)
  - [CDP Public Cloud](#cdp-public-cloud)
  - [CDP Private Cloud](#cdp-private-cloud)
- [Storage Area](#storage-area)
  - [CDP Private Cloud](#cdp-private-cloud-1)

## Output Ports

### CDP Public Cloud

When deploying on CDP Public Cloud, the provisioner expects the name of the CDP Environment and the CDW Virtual Warehouse, in order to perform discovery of the necessary endpoints using the Cloudera SDK.

| Field                          | Name                  | Description                                         | Required |
|--------------------------------|-----------------------|-----------------------------------------------------|----------|
| `specific.cdpEnvironment`      | CDP Environment       | Name of the environment in CDP Public Cloud         | Yes      |
| `specific.cdpVirtualWarehouse` | CDW Virtual Warehouse | Name of the Impala CDW Virtual Warehouse to be used | Yes      |

**Simple table on CDP Public Cloud:**

```yaml
id: urn:dmb:dp:platform:demo-dp:0
name: demo-dp
domain: platform
environment: dev
version: 0.0.1
dataProductOwner: dataProductOwner
specific: {}
components:
- id: urn:dmb:cmp:platform:demo-dp:0:witboost_table_impala
  name: witboost_table_impala
  description: description
  version: 0.0.1
  dataContract:
    schema:
    - name: id
      dataType: STRING
    - name: name
      dataType: STRING
    - name: country
      dataType: STRING
  specific: 
    databaseName: platform_demo_dp_0
    tableName: platform_demo_dp_0_witboost_table_impala_poc
    cdpEnvironment: sdp-aw-d-ir-env1
    cdwVirtualWarehouse: sdpawdir-sfs-i
    format: CSV
    location: s3a://bucket/path/to/folder/
```

**Partitioned table on CDP Public Cloud:**

Partitions are defined as a list of column names to be used as partitions. These columns must exist in the `dataContract.schema` field, which will be validated before deploying any resource.

```yaml
id: urn:dmb:dp:platform:demo-dp:0
name: demo-dp
domain: platform
environment: dev
version: 0.0.1
dataProductOwner: dataProductOwner
specific: {}
components:
- id: urn:dmb:cmp:platform:demo-dp:0:witboost_table_impala
  name: witboost_table_impala
  description: description
  version: 0.0.1
  dataContract:
    schema:
    - name: id
      dataType: STRING
    - name: name
      dataType: STRING
    - name: country
      dataType: STRING
  specific: 
    databaseName: platform_demo_dp_0
    tableName: platform_demo_dp_0_witboost_table_impala_poc
    cdpEnvironment: sdp-aw-d-ir-env1
    cdwVirtualWarehouse: sdpawdir-sfs-i
    format: CSV
    location: s3a://bucket/path/to/folder/
    partitions:
      - country
```

**Extra table parameters:**

The provisioner allows for an extra set of parameters to pass when the External Table is created for either an Output Port or a Storage. This allows to set custom TBLPROPERTIES, and for TEXTFILE and CSV files, to define a delimiter and whether the data files have a header at the first row.

| Field                                | Name                 | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | Required                                                                                                                    |
|--------------------------------------|----------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------|
| `specific.tableParams.header`        | Includes Header      | Used only for `CSV` and `TEXTFILE` formats, boolean value indicating whether the data files include a header on the first row.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | No. If not provided `false` is assumed                                                                                      |
| `specific.tableParams.delimiter`     | Field delimiter      | Used only for `TEXTFILE` formats. Delimiters are defined as any single ASCII character with integer value between 0 and 255 inclusive following the Impala definition. As these include non-printable characters, in order to allow the provisioner to receive the whole range of ASCII values, it expects a **quote-escaped** hex number in the range `"0x00"` to `"0xff"` inclusive. If the delimiter is a printable character (these usually are in the range of ASCII 32 to 126), it is possible to send it as is, so `delimiter: ","` is valid, while `delimiter: "\0"` is not.<br/>The format `CSV` is a shorthand for `format: TEXTFILE` with `delimiter: ","`. For this reason, if `CSV` is set as the format, any delimiter that is set on `tableParams.delimiter` will be ignored. | No. If not provided for `format: TEXTFILE`, the Hadoop default delimiter `"0x01"` will be used (otherwise known as Ctrl-A). |
| `specific.tableParams.tblProperties` | Impala TBLPROPERTIES | String key-value pair set of values. When provided, in case of any conflict with default TBLPROPERTIES added by the provisioner the user defined ones will take priority.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | No                                                                                                                          |

**Example input:**

```yaml
...
components:
- id: ...
  specific:
    databaseName: platform_demo_dp_0
    tableName: platform_demo_dp_0_witboost_table_impala_poc
    cdpEnvironment: sdp-aw-d-ir-env1
    cdwVirtualWarehouse: sdpawdir-sfs-i
    format: TEXTFILE
    location: s3a://bucket/path/to/folder/ # Specifies the HDFS folder path
    partitions: []
    tableParams:
      header: true 
      delimiter: "|"
      tblProperties:
        bucketing_version: "2"
```

Another example:

```yaml
...
components:
- id: ...
  specific:
    databaseName: platform_demo_dp_0
    tableName: platform_demo_dp_0_witboost_table_impala_poc
    cdpEnvironment: sdp-aw-d-ir-env1
    cdwVirtualWarehouse: sdpawdir-sfs-i
    format: TEXTFILE
    location: s3a://bucket/path/to/folder/ # Specifies the HDFS folder path
    partitions: []
    tableParams:
      delimiter: "0xfe"
```

### CDP Private Cloud

#### External Table

When working with a Private Cloud, the descriptor holds the same schema as the ones presented above, excepting some fields in the `specific` section:

```yaml
...
components:
- id: ...
  specific:
    databaseName: platform_demo_dp_0
    tableName: platform_demo_dp_0_witboost_table_impala_poc
    # cdpEnvironment: Omitted on CDP Private
    # cdwVirtualWarehouse: Omitted on CDP Private 
    format: CSV
    location: /path/to/folder/ # Specifies the HDFS folder path
    partitions:
      - country
```

The extra table parameters functionality explained on the CDP Public section applies as well for CDP Private Cloud with the same capabilities and considerations.

#### View

The provisioner allows to create simple views based on an origin table and the data contract schema. The specific field is as follows:

```yaml
...
components:
- id: ...
  specific:
    databaseName: platform_demo_dp_0
    tableName: platform_demo_dp_0_witboost_table_impala_poc # Table to be exposed through the view, belonging to the same DB 
    viewName: platform_demo_dp_0_witboost_external_view_poc
```

#### View

The provisioner allows to create simple views based on an origin table and the data contract schema. The specific field is as follows:

```yaml
...
components:
- id: ...
  specific:
    databaseName: platform_demo_dp_0
    tableName: platform_demo_dp_0_witboost_table_impala_poc # Table to be exposed through the view, belonging to the same DB 
    viewName: platform_demo_dp_0_witboost_external_view_poc
```

The database and tables must exist before provisioning the view. Currently, the provisioner only exposes existing columns (either all of them or a subset) based on the `dataContract.schema` field, so the column names must match to the names of the columns in the source `tableName`.

## Storage Area

Storage Areas create external tables on data that are only accessible within the data product but are not exposed to external users as it doesn't create an user role nor supports the Update ACL task. Currently, Storage Areas are only supported for CDP Private Cloud.

### CDP Private Cloud

#### External Table

The Storage Area descriptor for an External Table has a very similar shape to the Output Port, but the column schema is present on `specific.tableSchema` rather than `dataContract.schema`. Nonetheless, the way to write the schema is exactly the same.

```yaml
id: urn:dmb:dp:platform:demo-dp:0
name: demo-dp
domain: platform
environment: dev
version: 0.0.1
dataProductOwner: dataProductOwner
specific: {}
components:
- id: urn:dmb:cmp:platform:demo-dp:0:witboost_table_impala
  name: witboost_table_impala
  description: description
  owners: []
  specific: 
    databaseName: platform_demo_dp_0
    tableName: platform_demo_dp_0_witboost_table_impala_poc
    format: CSV
    location: /bucket/path/to/folder # Specifies the HDFS folder path
    tableSchema:
    - name: id
      dataType: STRING
    - name: name
      dataType: STRING
    - name: surname
      dataType: STRING
```

The extra table parameters functionality explained on the CDP Public section for Output Port External Tables applies as well for CDP Private Cloud Storage Areas with the same capabilities and considerations.

#### View

The View Storage Area has different capabilities than the View Output Port. To allow for flexibility on the creation of Impala data stacks, this view allows the user to provide a custom DML query statement in the form `SELECT ...`, allowing the user to define complex queries that don't map 1 to 1 to another existing output port or storage. The field for this is defined as follows:

| Field                     | Name                        | Description                                                                                                                                                                 | Required |
|---------------------------|-----------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| `specific.queryStatement` | Custom SQL SELECT Statement | DML Statement used at creation time of the view to define it. The table must be written using its fully qualified name, as these queries are executed at Impala root level. | Yes      |

⚠ **Warning**: As of the current version, this SQL statement is not analysed, so it may be vulnerable to SQL injection techniques ⚠

```yaml
id: urn:dmb:dp:platform:demo-dp:0
name: demo-dp
domain: platform
environment: dev
version: 0.0.1
dataProductOwner: dataProductOwner
specific: {}
components:
- id: urn:dmb:cmp:platform:demo-dp:0:witboost_table_impala
  name: witboost_table_impala
  description: description
  owners: []
  specific: 
    databaseName: platform_demo_dp_0
    viewName: platform_demo_dp_0_witboost_table_impala_poc
    queryStatement: "SELECT * FROM db.readsFromTableName WHERE country='UK'"
```

## Limitations

* Storage Areas can currently be provisioned only on CDP Private Cloud.
* On CDP Public Cloud, `location` needs to be expressed as `s3a://` and always ending with a slash `/`.
    * Each Data Product data must be located in a different bucket
* On CDP Private Cloud, `location` is expressed as the directory path on HDFS in the form `/path/to/folder/`.
* Schema data types only support primitive types TINYINT, SMALLINT, INT, BIGINT, DOUBLE, DECIMAL, TIMESTAMP, DATE, STRING, CHAR, VARCHAR and BOOLEAN, and ARRAY, MAP and STRUCT with these types

