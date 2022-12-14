package tech.ydb.importer.config;

/**
 * Source database types.
 * @see AnyTableLister.getInstance() for configuration based on it.
 * @author mzinal
 */
public enum SourceType {
    ORACLE,
    POSTGRESQL,
    MYSQL,
    MSSQL,
    DB2,
    INFORMIX,
}
