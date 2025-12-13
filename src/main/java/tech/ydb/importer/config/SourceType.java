package tech.ydb.importer.config;

/**
 * Source database types.
 * @see AnyTableLister.getInstance() for configuration based on it.
 * @author zinal
 */
public enum SourceType {
    GENERIC,
    CLICKHOUSE,
    ORACLE,
    POSTGRESQL,
    MYSQL,
    MSSQL,
    DB2,
    INFORMIX,
}
