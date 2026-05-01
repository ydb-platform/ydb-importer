package tech.ydb.importer.integration.verification;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.function.LongConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Shared INSERT helpers used by dialect loaders */
public final class LoaderUtil {

    private static final Logger LOG = LoggerFactory.getLogger(LoaderUtil.class);

    private static final int MAX_BLOB_BATCH = 50;
    private static final int MAX_PARAMS_PER_STATEMENT = 30_000;
    private static final int INSERT_MAX_ATTEMPTS = 3;

    private LoaderUtil() {
    }

    /**
     * Inserts every row of the scenario
     */
    public static void loadRows(Connection conn, TableScenario scenario,
                                int rowsPerStatement, LongConsumer onBatchFlush)
            throws SQLException {
        if (scenario.blobColumn() != null) {
            loadRowsBatched(conn, scenario, rowsPerStatement,
                    BlobInserter.DEFAULT, onBatchFlush);
        } else {
            loadRowsMultiRow(conn, scenario, rowsPerStatement, onBatchFlush);
        }
    }

    /** Inserts scenario rows with the given BlobInserter */
    public static void loadRows(Connection conn, TableScenario scenario,
                                int batchSize, BlobInserter blobInserter,
                                LongConsumer onBatchFlush) throws SQLException {
        loadRowsBatched(conn, scenario, batchSize, blobInserter, onBatchFlush);
    }

    /** Inserts scenario rows, for dialects without multi-row INSERT */
    public static void loadRowsBatched(Connection conn, TableScenario scenario,
                                       int batchSize, LongConsumer onBatchFlush)
            throws SQLException {
        loadRowsBatched(conn, scenario, batchSize, BlobInserter.DEFAULT, onBatchFlush);
    }

    /** Inserts many rows per INSERT statement */
    private static void loadRowsMultiRow(Connection conn, TableScenario scenario,
                                         int rowsPerStatement,
                                         LongConsumer onBatchFlush)
            throws SQLException {
        List<ColumnSpec> cols = scenario.columns();
        RowOracle oracle = scenario.oracle();
        long total = oracle.rowCount();
        int colCount = cols.size();
        int paramCap = Math.max(1, MAX_PARAMS_PER_STATEMENT / colCount);
        int fullChunk = Math.max(1, Math.min(rowsPerStatement, paramCap));

        String fullSql = buildMultiRowSql(scenario.tableName(), cols, fullChunk);
        try (PreparedStatement ps = conn.prepareStatement(fullSql)) {
            long id = 1;
            while (id + fullChunk - 1 <= total) {
                executeChunkWithRetry(ps, scenario.tableName(),
                        cols, oracle, id, fullChunk);
                onBatchFlush.accept(fullChunk);
                id += fullChunk;
            }
            long remaining = total - id + 1;
            if (remaining > 0) {
                String tailSql = buildMultiRowSql(scenario.tableName(), cols, (int) remaining);
                try (PreparedStatement tailPs = conn.prepareStatement(tailSql)) {
                    executeChunkWithRetry(tailPs, scenario.tableName(),
                            cols, oracle, id, (int) remaining);
                    onBatchFlush.accept(remaining);
                }
            }
        }
    }

    /** Runs the INSERT, retries if the driver returns the wrong row count */
    private static void executeChunkWithRetry(PreparedStatement ps, String table,
                                              List<ColumnSpec> cols,
                                              RowOracle oracle,
                                              long startId, int rows)
            throws SQLException {
        int lastInserted = -1;
        for (int attempt = 1; attempt <= INSERT_MAX_ATTEMPTS; attempt++) {
            fillChunk(ps, cols, oracle, startId, rows);
            lastInserted = ps.executeUpdate();
            if (lastInserted == rows) {
                return;
            }
            LOG.warn("Insert mismatch in {} id=[{}..{}] expected={} inserted={} (attempt {}/{})",
                    table, startId, startId + rows - 1, rows, lastInserted,
                    attempt, INSERT_MAX_ATTEMPTS);
        }
        throw new SQLException("Insert mismatch in " + table
                + " id=[" + startId + ".." + (startId + rows - 1)
                + "] expected=" + rows + " inserted=" + lastInserted
                + " after " + INSERT_MAX_ATTEMPTS + " attempts");
    }

    private static String buildMultiRowSql(String table, List<ColumnSpec> cols,
                                           int rows) {
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(table).append(" (");
        for (int i = 0; i < cols.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(cols.get(i).name());
        }
        sql.append(") VALUES ");
        for (int r = 0; r < rows; r++) {
            if (r > 0) {
                sql.append(',');
            }
            sql.append('(');
            for (int c = 0; c < cols.size(); c++) {
                if (c > 0) {
                    sql.append(',');
                }
                sql.append('?');
            }
            sql.append(')');
        }
        return sql.toString();
    }

    /** Sets parameters for all rows in the multi-row INSERT */
    private static void fillChunk(PreparedStatement ps, List<ColumnSpec> cols,
                                  RowOracle oracle, long startId, int rows)
            throws SQLException {
        int idx = 1;
        for (long id = startId; id < startId + rows; id++) {
            Map<String, Object> row = oracle.expectedFor(id);
            for (int c = 0; c < cols.size(); c++) {
                ColumnSpec col = cols.get(c);
                col.type().set(ps, idx++, row.get(col.name()));
            }
        }
    }

    /** Inserts row-by-row, with optional BLOB column */
    private static void loadRowsBatched(Connection conn, TableScenario scenario,
                                        int batchSize, BlobInserter blobInserter,
                                        LongConsumer onBatchFlush) throws SQLException {
        List<ColumnSpec> cols = scenario.columns();
        RowOracle oracle = scenario.oracle();
        boolean hasBlob = scenario.blobColumn() != null;
        int batch = hasBlob
                ? Math.min(batchSize, MAX_BLOB_BATCH) : batchSize;

        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(scenario.tableName()).append(" (");
        for (int i = 0; i < cols.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(cols.get(i).name());
        }
        if (hasBlob) {
            sql.append(", ").append(scenario.blobColumn());
        }
        sql.append(") VALUES (");
        int paramCount = cols.size() + (hasBlob ? 1 : 0);
        for (int i = 0; i < paramCount; i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append('?');
        }
        sql.append(')');

        try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            long pending = 0;
            for (long id = 1; id <= oracle.rowCount(); id++) {
                Map<String, Object> row = oracle.expectedFor(id);
                for (int c = 0; c < cols.size(); c++) {
                    ColumnSpec col = cols.get(c);
                    col.type().set(ps, c + 1, row.get(col.name()));
                }
                if (hasBlob) {
                    blobInserter.set(conn, ps, cols.size() + 1,
                            oracle.expectedBlobFor(id));
                }
                ps.addBatch();
                pending++;
                if (id % batch == 0) {
                    ps.executeBatch();
                    onBatchFlush.accept(pending);
                    pending = 0;
                }
            }
            if (pending > 0) {
                ps.executeBatch();
                onBatchFlush.accept(pending);
            }
        }
    }

    /** Sets a BLOB parameter, overridable per dialect, default uses setBytes */
    @FunctionalInterface
    public interface BlobInserter {

        BlobInserter DEFAULT = (conn, ps, index, blob) -> ps.setBytes(index, blob);

        void set(Connection conn, PreparedStatement ps, int index,
                 byte[] blob) throws SQLException;
    }
}
