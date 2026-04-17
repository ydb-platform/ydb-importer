package tech.ydb.importer.integration.verification;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.Set;
import java.util.function.LongConsumer;

/** Per-dialect adapter: creates a table and loads rows for a {@link TableScenario}. */
public interface DialectLoader {

    /** Creates a table for the scenario in the source DB. */
    void createTable(Connection conn, TableScenario scenario)
            throws SQLException;

    /**
     * Inserts all rows from the scenario oracle using JDBC batches.
     * {@code onBatchFlush} is invoked after each flushed batch with the number of rows in it.
     */
    default void loadRows(Connection conn, TableScenario scenario,
                          int batchSize, LongConsumer onBatchFlush)
            throws SQLException {
        if (supportsMultiRowInsert()) {
            LoaderUtil.loadRows(conn, scenario, batchSize, onBatchFlush);
        } else {
            LoaderUtil.loadRowsBatched(conn, scenario, batchSize, onBatchFlush);
        }
    }

    /** Adjusts the scenario table name for the dialect */
    default String effectiveTableName(String name) {
        return name;
    }

    default int rowsPerInsertStatement() {
        return 50_000;
    }

    default int loaderPoolSize() {
        return -1;
    }

    default boolean supportsMultiRowInsert() {
        return true;
    }

    default Object adjustExpected(LogicalType type, Object expected) {
        return expected;
    }

    /** True if the connection needs SET SCHEMA before DDL/DML. */
    default boolean requiresSetSchema() {
        return false;
    }

    /** Partition styles the dialect can express. Others fall back via {@link #fallbackOrder()}. */
    default Set<PartitionStyle> supportedPartitions() {
        return EnumSet.allOf(PartitionStyle.class);
    }

    /** Fallback partition styles tried in order when the scenario style is unsupported. */
    default PartitionStyle[] fallbackOrder() {
        return new PartitionStyle[] {
                PartitionStyle.HASH_INT,
                PartitionStyle.RANGE_INT
        };
    }
}
