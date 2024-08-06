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
* the degree of parallelism (which determines the worker pool size, plus the connection pool sizes for both source and target databases).

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

## 4. BLOB data import

For each BLOB field in the source database the import tool creates an additional YDB target table (BLOB supplemental table), having the following structure:
```sql
CREATE TABLE `blob_table`(
    `id` Int64,
    `pos` Int32,
    `val` String,
    PRIMARY KEY(`id`, `pos`)
)
```

The name of the BLOB supplemental table is generated based on the `table-options` / `blob-name-format` setting in the configuration file.

Each BLOB field value is saved as a sequence if records in the BLOB supplemental table in YQB.
Actual data is stored in the `val` field, storing no more than 64K bytes.
The order of blocks stored is defined by the values of the `pos` column, containing the integer values `1..N`.

A unique value of type `Int64` is generated for each source BLOB value, and stored in the `id` field of the BLOB supplemental table.
This identifier is also stored in the "main" target table in the field having the same name as the BLOB field in the source table.

## 5. Configuration file format

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
        <!-- Number of worker threads (integer starting with 1).
             This setting defines the maximum number of source and target database sessions, too.
         -->
        <pool size="4"/>
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
             Possible values: DATE (use YDB Date datatype, default), INT, STR.
             DATE does not support input values before January, 1, 1970.
             INT saves date as 32-bit integer YYYYMMDD for dates,
                 and as a 64-bit milliseconds since epoch for timestamps.
             STR saves dates as character strings (Utf8) in format "YYYY-MM-DD",
                 and in "YYYY-MM-DD hh:mm:ss.xxx" for timestamps.
         -->
        <conv-date>INT</conv-date>
        <conv-timestamp>STR</conv-timestamp>
        <!-- If true, columns with unsupported types are skipped with warning,
             otherwise import error is generated, and the whole table is skipped. -->
        <skip-unknown-types>true</skip-unknown-types>
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
    </table-ref>
</ydb-importer>
```

## 6. Building the tool from the source code

Java 11 or higher is required to build and run the tool. Maven is required to build the tool (tested on version 3.8.6).

To build the tool run the following command in the directory with the source code:
```bash
mvn package
```

After build is complete, the `target` subdirectory will contain the file `ydb-importer-X.Y-SNAPSHOT-bin.zip`, where `X.Y` is the version number.
