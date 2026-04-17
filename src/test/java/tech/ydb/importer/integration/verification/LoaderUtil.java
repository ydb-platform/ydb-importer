package tech.ydb.importer.integration.verification;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.function.LongConsumer;

/** Shared INSERT helpers used by dialect loaders. */
public final class LoaderUtil {

    private static final int MAX_BLOB_BATCH = 50;
    private static final int MAX_PARAMS_PER_STATEMENT = 30_000;
    private static final int INSERT_MAX_ATTEMPTS = 3;

    private LoaderUtil() {
    }

    /**
     * Inserts all scenario rows. Non-BLOB scenarios use multi-row INSERTs
     * with {@code rowsPerStatement} tuples per statement; BLOB scenarios fall
     * back to PreparedStatement batches.
     * {@code onBatchFlush} is invoked after each flushed batch with the number of rows in it.
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

    /** BLOB variant: PreparedStatement batches with a custom BLOB binder. */
    public static void loadRows(Connection conn, TableScenario scenario,
                                int batchSize, BlobInserter blobInserter,
                                LongConsumer onBatchFlush) throws SQLException {
        loadRowsBatched(conn, scenario, batchSize, blobInserter, onBatchFlush);
    }

    /** Forces PreparedStatement batch path (for dialects without multi-row INSERT). */
    public static void loadRowsBatched(Connection conn, TableScenario scenario,
                                       int batchSize, LongConsumer onBatchFlush)
            throws SQLException {
        loadRowsBatched(conn, scenario, batchSize, BlobInserter.DEFAULT, onBatchFlush);
    }

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

    private static void executeChunkWithRetry(PreparedStatement ps, String table,
                                              List<ColumnSpec> cols,
                                              RowOracle oracle,
                                              long startId, int rows)
            throws SQLException {
        int lastInserted = -1;
        for (int attempt = 1; attempt <= INSERT_MAX_ATTEMPTS; attempt++) {
            bindChunk(ps, cols, oracle, startId, rows);
            lastInserted = ps.executeUpdate();
            if (lastInserted == rows) {
                return;
            }
            System.err.printf(
                    "Insert mismatch in %s id=[%d..%d] expected=%d inserted=%d (attempt %d/%d)%n",
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

    private static void bindChunk(PreparedStatement ps, List<ColumnSpec> cols,
                                  RowOracle oracle, long startId, int rows)
            throws SQLException {
        int idx = 1;
        for (long id = startId; id < startId + rows; id++) {
            Map<String, Object> row = oracle.expectedFor(id);
            for (int c = 0; c < cols.size(); c++) {
                ColumnSpec col = cols.get(c);
                col.type().bind(ps, idx++, row.get(col.name()));
            }
        }
    }

    private static void loadRowsBatched(Connection conn, TableScenario scenario,
                                        int batchSize, BlobInserter blobInserter,
                                        LongConsumer onBatchFlush) throws SQLException {
        List<ColumnSpec> cols = scenario.columns();
        RowOracle oracle = scenario.oracle();
        boolean hasBlob = scenario.blobColumn() != null;
        int effectiveBatch = hasBlob
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
                    col.type().bind(ps, c + 1, row.get(col.name()));
                }
                if (hasBlob) {
                    blobInserter.bind(conn, ps, cols.size() + 1,
                            oracle.expectedBlobFor(id));
                }
                ps.addBatch();
                pending++;
                if (id % effectiveBatch == 0) {
                    ps.executeBatch();
                    onBatchFlush.accept(pending);
                    pending = 0;
                }
            }
            ps.executeBatch();
            if (pending > 0) {
                onBatchFlush.accept(pending);
            }
        }
    }
}
