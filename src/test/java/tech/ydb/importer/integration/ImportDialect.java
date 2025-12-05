package tech.ydb.importer.integration;

import java.util.List;

import org.testcontainers.containers.JdbcDatabaseContainer;

import tech.ydb.importer.config.SourceType;

/**
 * Abstraction over a JDBC source dialect.
 * Implementations provide container, schema/data preparation and a set of
 * import cases.
 */
public interface ImportDialect {

    /** Dialect name */
    String name();

    /** SourceType enum value */
    SourceType sourceType();

    /** JDBC driver class name */
    String getJdbcDriverClass();

    /** Create a Testcontainers JDBC database container */
    JdbcDatabaseContainer<?> createContainer();

    /** Test cases */
    List<DialectCase> cases();
}
