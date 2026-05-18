package tech.ydb.importer.integration.tabletest;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.importer.TableDecision;
import tech.ydb.importer.config.ImporterConfig;
import tech.ydb.importer.config.TableOptions;
import tech.ydb.importer.config.TableRef;
import tech.ydb.importer.integration.common.YdbImporterRunner;
import tech.ydb.importer.integration.common.YdbRowMatcher;
import tech.ydb.importer.integration.common.YdbSchemaReader;
import tech.ydb.importer.integration.common.YdbSchemaReader.YdbTableInfo;
import tech.ydb.table.values.Type;
import tech.ydb.importer.source.AnyTableLister;
import tech.ydb.importer.source.ColumnInfo;
import tech.ydb.importer.source.TableMapList;
import tech.ydb.importer.source.TableMetadata;

/** Fluent builder for table-level integration tests with raw SQL setup */
public final class TableTestBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(TableTestBuilder.class);

    private final AbstractYdbImporterTableTest test;
    final String schema;
    final String table;
    private String setupSql;
    private String cleanupSql;
    private String columns;
    private String tableSuffix;
    private String values;

    private static final String SYNTH_KEY_COLUMN = "ydb_synth_key";

    private Optional<List<String>> expectedPrimaryKey = Optional.empty();
    private boolean expectSyntheticKey;
    private Optional<Long> expectedRowCount = Optional.empty();
    private final List<String> expectedSkippedColumns = new ArrayList<>();
    private final Map<String, Type> expectedColumnTypes = new HashMap<>();
    private final List<Object[]> expectedRows = new ArrayList<>();
    private final List<String> expectedBlobColumns = new ArrayList<>();
    private final List<BlobCheck> blobChecks = new ArrayList<>();
    private final List<SetupAction> rowInserts = new ArrayList<>();

    @FunctionalInterface
    private interface BlobCheck {
        void verify(YdbSchemaReader ydb, String targetPath);
    }

    /** Post-setup hook for data that pure SQL cannot express (large BLOBs) */
    @FunctionalInterface
    public interface SetupAction {
        void run(Connection con) throws Exception;
    }

    private SetupAction setupCallback;
    private Consumer<TableOptions> optionsCustomizer = opts -> { };
    private Optional<Integer> fetchSize = Optional.empty();
    private String queryText;
    private String splitBy;
    private String splitFrom;
    private String splitTo;
    private int splitCount;
    private Integer ydbPartitionCount;
    private String ydbPartitionFrom;
    private String ydbPartitionTo;
    private Boolean useSourcePartitions;
    private Optional<Integer> expectedPartitionCount = Optional.empty();

    TableTestBuilder(AbstractYdbImporterTableTest test, String schema,
                     String table) {
        this.test = test;
        this.schema = schema;
        this.table = table;
    }

    public TableTestBuilder setupSql(String sql) {
        this.setupSql = sql;
        return this;
    }

    public TableTestBuilder cleanupSql(String sql) {
        this.cleanupSql = sql;
        return this;
    }

    public TableTestBuilder setupCallback(SetupAction action) {
        this.setupCallback = action;
        return this;
    }

    /** Parameterized INSERT using setObject, for values not expressible as SQL literals */
    public TableTestBuilder insertRow(String sql, Object... values) {
        this.rowInserts.add(con -> {
            try (java.sql.PreparedStatement ps = con.prepareStatement(sql)) {
                for (int i = 0; i < values.length; i++) {
                    ps.setObject(i + 1, values[i]);
                }
                ps.executeUpdate();
            }
        });
        return this;
    }

    public TableTestBuilder columns(String columnDefs) {
        this.columns = columnDefs;
        return this;
    }

    public TableTestBuilder tableSuffix(String suffix) {
        this.tableSuffix = suffix;
        return this;
    }

    public TableTestBuilder values(String valuesCsv) {
        this.values = valuesCsv;
        return this;
    }

    public TableTestBuilder withOptions(Consumer<TableOptions> customizer) {
        this.optionsCustomizer = customizer == null ? opts -> { } : customizer;
        return this;
    }

    public TableTestBuilder fetchSize(int value) {
        this.fetchSize = Optional.of(value);
        return this;
    }

    public TableTestBuilder queryText(String sql) {
        this.queryText = sql;
        return this;
    }

    public TableTestBuilder splitBy(String column) {
        this.splitBy = column;
        return this;
    }

    public TableTestBuilder splitFrom(String value) {
        this.splitFrom = value;
        return this;
    }

    public TableTestBuilder splitTo(String value) {
        this.splitTo = value;
        return this;
    }

    public TableTestBuilder splitCount(int count) {
        this.splitCount = count;
        return this;
    }

    public TableTestBuilder ydbPartitionCount(Integer count) {
        this.ydbPartitionCount = count;
        return this;
    }

    public TableTestBuilder ydbPartitionFrom(String value) {
        this.ydbPartitionFrom = value;
        return this;
    }

    public TableTestBuilder ydbPartitionTo(String value) {
        this.ydbPartitionTo = value;
        return this;
    }

    public TableTestBuilder useSourcePartitions(Boolean value) {
        this.useSourcePartitions = value;
        return this;
    }

    void applyConfigTo(TableRef ref) {
        if (splitBy != null) {
            ref.setSplitBy(splitBy);
            ref.setSplitFrom(splitFrom);
            ref.setSplitTo(splitTo);
            ref.setSplitCount(splitCount);
        }
        if (ydbPartitionCount != null) {
            ref.setYdbPartitionCount(ydbPartitionCount);
        }
        if (ydbPartitionFrom != null) {
            ref.setYdbPartitionFrom(ydbPartitionFrom);
        }
        if (ydbPartitionTo != null) {
            ref.setYdbPartitionTo(ydbPartitionTo);
        }
        if (useSourcePartitions != null) {
            ref.setUseSourcePartitions(useSourcePartitions);
        }
    }

    public TableTestBuilder expectPrimaryKey(String... columns) {
        this.expectedPrimaryKey = Optional.of(Arrays.asList(columns));
        return this;
    }

    public TableTestBuilder expectSyntheticKey() {
        this.expectSyntheticKey = true;
        return this;
    }

    public TableTestBuilder expectRowCount(long count) {
        this.expectedRowCount = Optional.of(count);
        return this;
    }

    public TableTestBuilder expectPartitionCount(int count) {
        this.expectedPartitionCount = Optional.of(count);
        return this;
    }

    public TableTestBuilder expectSkippedColumns(String... names) {
        for (String name : names) {
            this.expectedSkippedColumns.add(name);
        }
        return this;
    }

    public TableTestBuilder expectColumn(String name, Type type) {
        this.expectedColumnTypes.put(name, type);
        return this;
    }

    /** Asserts at least one row matches all given column-value pairs */
    public TableTestBuilder expectRowExists(Object... columnValuePairs) {
        this.expectedRows.add(columnValuePairs);
        return this;
    }

    public TableTestBuilder expectBlobColumn(String columnName) {
        this.expectedBlobColumns.add(columnName);
        return this;
    }

    public TableTestBuilder expectBlobBytes(String column, String pkColumn,
                                            Object pkValue, byte[] bytes) {
        blobChecks.add((ydb, targetPath) -> {
            byte[] actual = ydb.readBlobBytes(targetPath, column, pkColumn, pkValue);
            assertArrayEquals(bytes, actual,
                    "BLOB " + column + " for " + pkColumn + "=" + pkValue);
        });
        return this;
    }

    public void run() throws Exception {
        buildDdlIfNeeded();

        String targetPath = YdbImporterRunner.DEFAULT_TARGET_PREFIX
                + "." + schema + "." + table;

        YdbImporterRunner.Builder builder = importerRunnerBuilder();
        try (YdbSchemaReader ydb = new YdbSchemaReader(
                test.ydbContainer().getConnectionString())) {
            try {
                executeSetup();
                verifyPhase1(builder);
                builder.run();
                verifyPhase2(ydb, targetPath);
            } finally {
                executeCleanup();
                dropBlobTables(ydb, targetPath);
                ydb.dropTable(targetPath);
            }
        }
    }

    void buildDdlIfNeeded() {
        if (setupSql != null) {
            return;
        }
        if (columns == null || columns.isEmpty()) {
            if (setupCallback != null) {
                return;
            }
            throw new IllegalStateException(
                    "Either setupSql, columns or setupCallback must be set");
        }
        String qualifiedName = schema + "." + table;
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE ").append(qualifiedName)
                .append(" (").append(columns).append(")");
        if (tableSuffix != null && !tableSuffix.isEmpty()) {
            ddl.append(" ").append(tableSuffix);
        }
        setupSql = ddl.toString();
        if (values != null && !values.isEmpty()) {
            setupSql += ";" + "INSERT INTO " + qualifiedName
                    + " VALUES " + values;
        }
        if (cleanupSql == null) {
            cleanupSql = "DROP TABLE IF EXISTS " + qualifiedName;
        }
    }

    void executeSetup() throws Exception {
        try (Connection con = test.openSourceConnection()) {
            if (setupSql != null) {
                for (String sql : splitStatements(setupSql)) {
                    try (Statement st = con.createStatement()) {
                        st.execute(sql);
                    }
                }
            }
            for (SetupAction action : rowInserts) {
                action.run(con);
            }
            if (setupCallback != null) {
                setupCallback.run(con);
            }
        }
    }

    void executeCleanup() {
        if (cleanupSql == null || cleanupSql.isEmpty()) {
            return;
        }
        List<String> statements = splitStatements(cleanupSql);
        try (Connection con = test.openSourceConnection()) {
            for (String sql : statements) {
                try (Statement st = con.createStatement()) {
                    st.execute(sql);
                } catch (Exception ex) {
                    LOG.warn("Cleanup statement failed: {}", sql, ex);
                }
            }
        } catch (Exception ex) {
            LOG.warn("Failed to open source connection for cleanup", ex);
        }
    }

    // Naive split - does not handle semicolons inside string literals
    private static List<String> splitStatements(String sql) {
        List<String> out = new ArrayList<>();
        for (String raw : sql.split(";")) {
            String trimmed = raw.trim();
            if (!trimmed.isEmpty()) {
                out.add(trimmed);
            }
        }
        return out;
    }

    private YdbImporterRunner.Builder importerRunnerBuilder() {
        YdbImporterRunner.Builder builder = YdbImporterRunner.builder()
                .source(test.sourceContainer(), test.sourceType())
                .ydb(test.ydbContainer())
                .table(schema, table)
                .customizeOptions(optionsCustomizer)
                .customizeTable(this::applyConfigTo)
                .useArrow(test.useArrow())
                .queryText(queryText);
        if (fetchSize.isPresent()) {
            builder.fetchSize(fetchSize.get());
        }
        return builder;
    }

    private void verifyPhase1(YdbImporterRunner.Builder builder) throws Exception {
        if (!hasPhase1Expectations()) {
            return;
        }

        ImporterConfig config = builder.buildConfig();

        try (Connection con = test.openSourceConnection()) {
            AnyTableLister lister = AnyTableLister.getInstance(
                    new TableMapList(config), con);
            TableDecision td = new TableDecision(
                    config.getTableRefs().get(0));
            TableMetadata tm = lister.readMetadata(con, td);
            td.setMetadata(tm);

            String label = schema + "." + table;

            expectedPrimaryKey.ifPresent(expected -> {
                List<String> actual = new ArrayList<>();
                for (ColumnInfo ci : tm.getKey()) {
                    actual.add(ci.getName());
                }
                assertEquals(expected, actual, "Source PK for " + label);
            });

            if (expectSyntheticKey) {
                assertTrue(tm.getKey().isEmpty(),
                        "Source PK should be empty for " + label);
            }
        }
    }

    private boolean hasPhase1Expectations() {
        return expectedPrimaryKey.isPresent() || expectSyntheticKey;
    }

    void verifyPhase2(YdbSchemaReader ydb, String targetPath) {
        YdbTableInfo info = ydb.describe(targetPath);

        expectedPrimaryKey.ifPresent(expected ->
                assertEquals(expected, info.getPrimaryKey(),
                        "YDB PK for " + targetPath));

        expectedPartitionCount.ifPresent(expected ->
                assertEquals(expected.intValue(), info.getPartitionCount(),
                        "YDB partition count for " + targetPath));

        if (expectSyntheticKey) {
            assertEquals(Collections.singletonList(SYNTH_KEY_COLUMN),
                    info.getPrimaryKey(),
                    "YDB synthetic key for " + targetPath);
            assertTrue(info.hasColumn(SYNTH_KEY_COLUMN),
                    "Synthetic key column missing in " + targetPath);
        }

        for (String skipped : expectedSkippedColumns) {
            assertFalse(info.hasColumn(skipped),
                    "Column " + skipped + " should be skipped in "
                    + targetPath);
        }

        for (Map.Entry<String, Type> entry : expectedColumnTypes.entrySet()) {
            assertEquals(entry.getValue(),
                    info.getColumn(entry.getKey()).getRawType(),
                    "YDB type for column " + entry.getKey()
                    + " in " + targetPath);
        }

        expectedRowCount.ifPresent(expected ->
                assertEquals(expected, ydb.countRows(targetPath),
                        "Row count for " + targetPath));

        if (!expectedRows.isEmpty()) {
            List<String> pk = info.getPrimaryKey();
            assertFalse(pk.isEmpty(),
                    "expectRowExists requires expectSyntheticKey or "
                    + "expectPrimaryKey to determine row ordering");
            List<Map<String, Object>> rows = ydb.readRows(
                    targetPath, pk.get(0));
            for (Object[] cvPairs : expectedRows) {
                YdbRowMatcher.assertRowExists(rows, cvPairs);
            }
        }

        for (String blobCol : expectedBlobColumns) {
            String auxPath = targetPath + "_" + blobCol;
            ydb.describe(auxPath);
            assertTrue(ydb.countRows(auxPath) > 0,
                    "Blob aux table " + auxPath
                    + " should have rows");
        }

        for (BlobCheck check : blobChecks) {
            check.verify(ydb, targetPath);
        }
    }

    void dropBlobTables(YdbSchemaReader ydb, String targetPath) {
        for (String blobCol : expectedBlobColumns) {
            ydb.dropTable(targetPath + "_" + blobCol);
        }
    }
}
