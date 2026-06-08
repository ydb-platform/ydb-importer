<img width="64" src="https://github.com/ydb-platform/ydb/raw/main/ydb/docs/_assets/logo.svg"/><br/>

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/ydb-platform/ydb-importer/blob/main/LICENSE)
[![Release](https://img.shields.io/github/v/release/ydb-platform/ydb-importer.svg?style=flat-square)](https://github.com/ydb-platform/ydb-importer/releases)

# Import data from JDBC data sources to YDB databases

This tool imports table structures and data records from a JDBC data source to YDB.

Right now the tested data sources include the following:

* [PostgreSQL](https://www.postgresql.org/),
* [MySQL](https://www.mysql.com/),
* [Oracle Database](https://www.oracle.com/database/),
* [Microsoft SQL Server](https://www.microsoft.com/sql-server/),
* [IBM Db2](https://www.ibm.com/products/db2),
* [IBM Informix](https://www.ibm.com/products/informix).

Other data sources will probably work, too, as the tool uses the generic JDBC APIs to retrieve data and metadata.

Some data types and table structures are known to be unsupported:

* the embedded tables for Oracle Database,
* spatial data type for Microsoft SQL Server,
* object data types for Informix.

## 1. Operating instructions

The tool reads the XML configuration file on startup. The file name needs to be specified as a command line agrument when running the tool.

Settings in the configuration file define:
* source database type and connection preferences;
* YDB target database connection preferences;
* file name to save the generated YQL script with YDB target tables structure;
* the rules to filter source tables' names;
* the rules to generate target tables' names based on the names of the source tables;
* the degree of parallelism (which determines the reader and writer pool sizes, plus the connection pool sizes for both source and target databases).

Data import is performed as the following sequence of actions:
1. The tool connects to the source database, and determines the list of tables and custom SQL queries to be imported.
2. The structure of target YDB tables is generated, and optionally saved as YQL script.
3. Target database is checked for existence of the target tables. Missing tables are created, already existing ones may be left as is, or re-created.
4. Row data is imported by reading from the source database using the SELECT statements, and written into the target YDB database using the Bulk Upsert mechanizm.

All operations, including metadata extraction from the source database, table creation (and re-creation) in the target database, and row data import, are performed in parallel mode (thread-per-table), using multiple concurrent threads and multiple open connections to both source and target databases. The maximum degree of parallelizm is configurable, although the actual number of concurrent operations cannot exceed the number of tables being imported.

## 2. Running the tool

The compiled tool is available as the ZIP archive, which contains the configuration file examples as `*.xml`, sample tool startup script `ydb-importer.sh`, and the executable code for the tool and its dependencies (including the YDB Java SDK) as `lib/*.jar` files.

Before running the tool the required JDBC drivers should be put (as `*.jar` files) into the `lib` subdirectory of the tool installation directory.

The configuration file needs to be prepared based on one of the provided samples.

The example command to run the tool is provided in the [ydb-importer.sh](scripts/ydb-importer.sh) file, which can also be used to run the tool as shown below:

```bash
./ydb-importer.sh my-import-config.xml
```

The name of the configuration file is provided as the command line parameter to the tool.

## 3. Handling tables without the primary key

Each YDB table must have a primary key. If a primary key (or at least a unique index) is defined on the source table, the tool creates the primary key for the target YDB table with the columns and order defined by the original primary key. When having multiple unique indexes defined on the source table, the tool prefers the index with the smallest number of columns.

In case the source table has no primary key and no unique indexes, primary key columns can be configured using the import settings in the configuration file, as a sequence of `key-column` elements in the `table-ref` section (see below the example in the description of the configuration file format).

When there is no primary key defined anywhere, the tool automatically adds the column `ydb_synth_key` to the target table, and uses it as the primary key. The values of the synthetic primary key are computed as "SHA-256" hash code over the values of all columns of the row, except the columns of the BLOB type.

If the input table contains several rows with completely identical values, the destination table will have only one row per each set of duplicate input rows. This also means that there the tool cannot import the table in which all columns are of BLOB type.

## 4. BLOB and CLOB data import

For each BLOB field in the source database the import tool creates an additional YDB target table (BLOB supplemental table), having the following structure:
```sql
CREATE TABLE `blob_table`(
    `id` Int64,
    `pos` Int32,
    `val` String,
    PRIMARY KEY(`id`, `pos`)
)
```

A similar table is created for each CLOB field:
```sql
CREATE TABLE `clob_table`(
    `id` Int64,
    `pos` Int32,
    `val` Text,
    PRIMARY KEY(`id`, `pos`)
)
```

A CLOB table is created for source columns of types CLOB or NCLOB, as well as for text columns explicitly marked with `<clob-column>` in the `<table-ref>` section.

The name of the supplemental table is generated based on the `table-options` / `blob-name-format` setting in the configuration file.

Each source field value is saved as a sequence of records in the supplemental table.
A unique value of type `Int64` is generated for each source value, and stored in the `id` field.
This identifier is also stored in the "main" target table in the field having the same name as the source field.
Each BLOB record stores no more than 64K bytes in the `val` field, each CLOB record stores no more than 32768 characters. The order of blocks is defined by the values of the `pos` column.

## 5. YDB partitioning and parallel source reading

The tool independently manages two aspects.

- **YDB target table partitioning** via `<ydb-partition-count>`. Sets the number of partitions for the YDB table created via `PARTITION_AT_KEYS`.
- **Parallel source reading**. How many parts the source queries are split into: by range over `<split-by>`/`<split-count>`, or by the source native partitions (`<use-source-partitions>`).

Both modes create tasks that run in parallel, with a failed task retried on error (see `<retry-count>`).

See the "Configuration file format" section for the details of each tag.

### How the two modes interact

Reading and target partitioning are resolved independently.

**Reading.** With `<split-by>` reading goes by range over the split column (`<use-source-partitions>` then has no effect). Without it, each source native partition is read by a separate task.

**Target partitioning.** When `<ydb-partition-count>=auto`, if the source has partitions whose first key column ranges do not overlap, the YDB table copies their count and boundaries. If `<split-by>` is over the same first key column, read slices line up with the YDB partitions, so each batch lands entirely in one of them.

**When they do not line up.** `<use-partition-buffers>` regroups rows on the reader side into one batch per YDB partition. It requires an integer leading key and resolved `<ydb-partition-count>` boundaries. Otherwise it falls back to plain batching.

## 6. Configuration file format

Sample configuration files:
* [for PostgreSQL](scripts/sample-postgres.xml);
* [for MySQL](scripts/sample-mysql.xml);
* [for Oracle Database](scripts/sample-oracle.xml);
* [for Microsoft SQL Server](scripts/sample-mssql.xml);
* [for IBM Db2](scripts/sample-db2.xml);
* [for IBM Informix](scripts/sample-informix.xml).

Below is the definition of the configuration file structure:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<ydb-importer>
    <workers>
        <!-- Number of reader threads (integer starting with 1).
             This setting defines the maximum number of source database sessions, too.
         -->
        <reader-pool size="4"/>
        <!-- Number of writer threads (integer starting with 1).
             This setting defines the maximum number of target database sessions, too.
             If not set, reader-pool size is used.
         -->
        <writer-pool size="4"/>
        <!-- Maximum number of pending batches between reader and writer threads.
             If not set, reader-pool size is used.
         -->
        <buffer-count>4</buffer-count>
        <!-- Use Apache Arrow columnar format for writes to YDB.
             Increases load speed. Requires YDB version 26.1 or later.
         -->
        <use-arrow>false</use-arrow>
    </workers>
    <!-- Source database connection parameters.
         type - the required attribute defining the type of the data source
      -->
    <source type="generic|postgresql|mysql|oracle|mssql|db2|informix">
        <!-- JDBC driver class name to be used. Typical values:
              org.postgresql.Driver
              com.mysql.cj.jdbc.Driver
              org.mariadb.jdbc.Driver
              oracle.jdbc.driver.OracleDriver
              com.microsoft.sqlserver.jdbc.SQLServerDriver
              com.ibm.db2.jcc.DB2Driver
              com.informix.jdbc.IfxDriver
        -->
        <jdbc-class>driver-class-name</jdbc-class>
        <!-- JDBC driver URL. Value templates:
              jdbc:postgresql://hostname:5432/dbname
              jdbc:mysql://hostname:3306/dbname
              jdbc:mariadb://hostname:3306/dbname
              jdbc:oracle:thin:@//hostname:1521/serviceName
              jdbc:sqlserver://localhost;encrypt=true;trustServerCertificate=true;database=AdventureWorks2022;
              jdbc:db2://localhost:50000/SAMPLE
              jdbc:informix-sqli://localhost:9088/stores_demo:INFORMIXSERVER=informix
        -->
        <jdbc-url>jdbc-url</jdbc-url>
        <username>username</username>
        <password>password</password>
        <!-- Number of retries per partition query on source read errors,
             with exponential backoff (1s, 2s, 4s, ..., capped at 30s).
             Applies to partitioned tables without BLOB or CLOB columns.
         -->
        <retry-count>10</retry-count>
    </source>
    <!-- Target YDB database connection parameters. -->
    <target type="ydb">
        <!-- If the following tag is defined, the tool will import
             the YQL script to generate YDB tables into the file specified.
             It can also be used without specifying the connection string,
             if the actual target schema creation is not required.
        -->
        <script-file>sample-database.yql.tmp</script-file>
        <!-- Connection string: protocol + endpoint + database. Typical values:
            grpcs://ydb.serverless.yandexcloud.net:2135?database=/ru-central1/b1gfvslmokutuvt2g019/etn63999hrinbapmef6g
            grpcs://localhost:2135?database=/local
            grpc://localhost:2136?database=/Root/testdb
         -->
        <connection-string>ydb-connection-string</connection-string>
        <!-- Authentication mode:
          ENV      Configure authentication through the environment
          NONE     Anonymous access, or static credentials in the connection string
          STATIC   Static credentials defined as login and password properties (see below)
          SAKEY    Service account key file authentication for YDB Managed Service
          METADATA Service account metadata authentication for YDB Managed Service
        -->
        <auth-mode>ENV</auth-mode>
        <!--
         For managed YDB in Yandex Cloud, authentication parameters can be specified
         in the environment variables, as specified in the documentation:
            https://ydb.tech/en/docs/reference/ydb-sdk/auth#env
         In case the Service Account authentication is used, either explicitly
         or through the YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS env, the key file
         must be generated as written in the following document:
            https://cloud.yandex.com/en/docs/iam/operations/authorized-key/create
        -->
        <!-- Custom TLS certificates, if needed -->
        <tls-certificate-file>ca.crt</tls-certificate-file>
        <!-- For auth-mode: STATIC -->
        <static-login>username</static-login>
        <static-password>password</static-password>
        <!-- For auth-mode: SAKEY -->
        <sa-key-file>ydb-sa-keyfile.json</sa-key-file>
        <!-- Drop the already existing tables before loading the data -->
        <replace-existing>true</replace-existing>
        <!-- Should the tool actually load the data after creating the tables? -->
        <load-data>true</load-data>
        <!-- Maximum rows per bulk upsert operation -->
        <max-batch-rows>1000</max-batch-rows>
        <!-- Maximum rows per blob bulk upsert operation -->
        <max-blob-rows>200</max-blob-rows>
    </target>
    <!-- Table name and structure conversion rules.
         Each rule is defined under a distinct name, and later referenced in the table mappings.
     -->
    <table-options name="default">
        <!--  Table name case conversion mode: ASIS (no changes, used by default), LOWER, UPPER. -->
        <case-mode>ASIS</case-mode>
        <!-- The template used to generate the full destination YDB table name,
             including the directory to put the table in.
             The values ${schema} and ${table} are used to substitute
             the schema and table name of the source table. -->
        <table-name-format>oraimp1/${schema}/${table}</table-name-format>
        <!-- The template used to generate the name of the supplemental table
             to store source BLOB field's data in YDB.
             The values ${schema}, ${table} and ${field}
             are used for source schema, table and BLOB field names. -->
        <blob-name-format>oraimp1/${schema}/${table}_${field}</blob-name-format>
        <!-- Date and timestamp data type values conversion mode.
             Possible values: DATE_NEW (YDB Date32/Timestamp64, default), DATE (YDB Date/Timestamp), INT, STR.
             DATE does not support input values before January, 1, 1970.
             INT saves date as 32-bit integer YYYYMMDD for dates,
                 and as a 64-bit milliseconds since epoch for timestamps.
             STR saves dates as character strings (Utf8) in format "YYYY-MM-DD",
                 and in "YYYY-MM-DD hh:mm:ss.xxx" for timestamps.
         -->
        <conv-date>DATE_NEW</conv-date>
        <conv-timestamp>DATE_NEW</conv-timestamp>
        <!-- Allow usage of Decimal(M,N) with custom M,N parameters
             (requires YDB 25.1 or newer). -->
        <allow-custom-decimal>true</allow-custom-decimal>
        <!-- If true, columns with unsupported types are skipped with warning,
             otherwise import error is generated, and the whole table is skipped. -->
        <skip-unknown-types>true</skip-unknown-types>
        <!-- Use source-side native partitions for parallel reads. Works for databases
             that support partitioning. Default is true. Can be overridden in <table-ref>. -->
        <use-source-partitions>true</use-source-partitions>
        <!-- Regroup rows on the reader side into per-YDB-partition batches, so each bulk
             upsert lands in a single partition. Default is true.
             Can be overridden in <table-ref>. -->
        <use-partition-buffers>true</use-partition-buffers>
        <!-- Initial number of YDB target table partitions (PARTITION_AT_KEYS in DDL).
             Applies only to an integer leading key column, otherwise skipped.
             auto - partition count by the first key column: equals split-count if set,
                    otherwise the number of source partitions if any (see section 5).
             N    - integer >= 2, split the first key column range into N equal
                    intervals (from ydb-partition-from/to, otherwise source MIN/MAX).
             none - do not set it, YDB manages partitions on its own.
             Default is auto. Can be overridden in <table-ref>. -->
        <ydb-partition-count>auto</ydb-partition-count>
    </table-options>
    <!-- Table map filters the source tables and defines the conversion modes for them. -->
    <table-map options="default">
        <!-- Schemas to include, can be specified as regular expression or literal value -->
        <include-schemas regexp="true">.*</include-schemas>
        <!-- Schemas to exclude (of those included), as literal or regular expression -->
        <exclude-schemas>SOMESCHEMA</exclude-schemas>
        <!-- The particular tables can be included or excluded using the
             "include-tables" and "exclude-tables" tags,
             using literals or regular expressions as well. -->
    </table-map>
    <!-- Table reference may refer to the particular table in the particular schema,
         and optionally specify the query to execute. The latter option allows to import
         the virtual tables which do not actually exist on the data source. -->
    <table-ref options="default">
        <schema-name>ora$sys</schema-name>
        <table-name>all_tables</table-name>
        <!-- If the query text is defined, it is executed as shown. -->
        <query-text>SELECT * FROM all_tables</query-text>
        <!-- Primary key columns should be defined for the query.  -->
        <key-column>OWNER</key-column>
        <key-column>TABLE_NAME</key-column>
        <!-- Splits the table into split-count value ranges along the
             split-by column for parallel reads. Column types:
             integers (TINYINT/SMALLINT/INTEGER/BIGINT), DECIMAL/NUMERIC,
             REAL/FLOAT/DOUBLE, DATE, TIMESTAMP. Bounds are written as
             yyyy-MM-dd for DATE or yyyy-MM-dd [HH:mm:ss[.fraction]] for
             TIMESTAMP. Values below split-from or NULL go to the first
             split, values above split-to go to the last.

             split-by also accepts the auto literal, meaning the first
             primary key column (resolved while reading metadata, if the
             table has no primary key or its leading column has an
             unsupported type, the split is disabled). split-by may be
             omitted when another split-* tag is set, it then defaults to auto.

             split-count, split-from, split-to can be set to auto: then split-count
             comes from ydb-partition-count, and split-from/to from the source MIN/MAX.
         -->
        <split-by>created_at</split-by>
        <split-from>2020-01-01 00:00:00</split-from>
        <split-to>2026-01-01 00:00:00</split-to>
        <split-count>auto</split-count>
        <!-- Per-table overrides of the partitioning settings from <table-options>.
             Same values as in <table-options>. -->
        <use-source-partitions>true</use-source-partitions>
        <use-partition-buffers>false</use-partition-buffers>
        <ydb-partition-count>16</ydb-partition-count>
        <!-- Explicit bounds of the first key column for splitting the YDB table
             into equal intervals. Used only with a numeric ydb-partition-count.
             If not set, MIN/MAX of the first key column is taken from the source. -->
        <ydb-partition-from>1</ydb-partition-from>
        <ydb-partition-to>1000000</ydb-partition-to>
        <!-- Marks a text column for import as CLOB. The column must
             have a text SQL type: CHAR, VARCHAR, NCHAR, NVARCHAR,
             LONGVARCHAR, LONGNVARCHAR.
         -->
        <clob-column>note</clob-column>
    </table-ref>
</ydb-importer>
```

## 7. Building the tool from the source code

Java 8 or higher is required to build and run the tool. Maven is required to build the tool (tested on version 3.8.6).

To build the tool run the following command in the directory with the source code:
```bash
mvn package
```

After build is complete, the `target` subdirectory will contain the file `ydb-importer-X.Y-SNAPSHOT-bin.zip`, where `X.Y` is the version number.
