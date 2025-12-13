package tech.ydb.importer.integration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.JdbcDatabaseContainer;

import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.importer.YdbImporter;
import tech.ydb.importer.config.ImporterConfig;
import tech.ydb.importer.config.SourceConfig;
import tech.ydb.importer.config.TableOptions;
import tech.ydb.importer.config.TableRef;
import tech.ydb.importer.config.TargetConfig;
import tech.ydb.importer.config.TargetScript;
import tech.ydb.importer.config.TargetType;
import tech.ydb.importer.config.WorkerConfig;
import tech.ydb.importer.config.YdbAuthMode;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import tech.ydb.table.description.TableColumn;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

/**
 * Common infrastructure for end-to-end import tests.
 * <p>
 * Handles local YDB Testcontainers lifecycle, builds configuration
 * and provides shared schema/data checks.
 */
public abstract class BaseImportIntegrationTest {

    private static final int WORKER_POOL_SIZE = 4;
    private static final int MAX_BATCH_ROWS = 1000;
    private static final int MAX_BLOB_ROWS = 200;
    private static final Duration YDB_STARTUP_TIMEOUT = Duration.ofSeconds(30);
    private static final String DEFAULT_TABLE_OPTIONS_NAME = "default";

    private static LocalYdbTestContainer ydb;

    @BeforeAll
    static void startYdb() throws Exception {
        ydb = new LocalYdbTestContainer();
        ydb.start();
        waitForYdbReady(ydb.getConnectionString(), YDB_STARTUP_TIMEOUT);
    }

    /**
     * Poll YDB until a simple query succeeds or the timeout expires.
     * This needs to ensure that the YDB instance has fully initialized.
     */
    private static void waitForYdbReady(String connectionString, Duration timeout) throws InterruptedException {
        final long deadlineNanos = System.nanoTime() + timeout.toNanos();

        while (System.nanoTime() < deadlineNanos) {
            try (GrpcTransport transport = GrpcTransport.forConnectionString(connectionString).build();
                    TableClient client = TableClient.newClient(transport).build()) {

                SessionRetryContext retryCtx = SessionRetryContext.create(client).build();

                Status status = retryCtx
                        .supplyStatus(session -> session.executeDataQuery(
                                "SELECT 1;",
                                TxControl.onlineRo())
                                .thenApply(Result::getStatus))
                        .join();

                if (status.isSuccess()) {
                    return;
                }
            } catch (Exception ignore) {

            }

            Thread.sleep(500);
        }

        throw new IllegalStateException("YDB is not ready within " + timeout.toMillis() + " ms");
    }

    @AfterAll
    static void stopYdb() {
        if (ydb != null) {
            ydb.stop();
        }
    }

    protected LocalYdbTestContainer getYdbContainer() {
        return ydb;
    }

    /**
     * Concrete test classes to run.
     */
    protected abstract List<ImportDialect> dialects();

    /**
     * Create TestYdbClient bound to the current test YDB container.
     */
    protected TestYdbClient createYdbClient() {
        return new TestYdbClient(getYdbContainer().getConnectionString());
    }

    /**
     * Utility to open a JDBC connection to a Testcontainers source DB.
     */
    protected Connection openSourceConnection(JdbcDatabaseContainer<?> source) throws Exception {
        Class.forName(source.getDriverClassName());
        return DriverManager.getConnection(
                source.getJdbcUrl(),
                source.getUsername(),
                source.getPassword());
    }

    /**
     * Build in-memory ImporterConfig
     */
    protected ImporterConfig buildImporterConfig(
            ImportDialect dialect,
            ImportCase testCase,
            JdbcDatabaseContainer<?> source) {
        ImporterConfig config = new ImporterConfig();

        configureWorkers(config);
        configureSource(config, dialect, source);
        configureTarget(config, testCase);
        configureTableOptions(config, dialect, testCase);
        configureTableRefs(config, testCase);

        return config;
    }

    protected void runImporter(ImporterConfig config) throws Exception {
        new YdbImporter(config).run();
    }

    /**
     * Assert that YDB table schema matches expected.
     */
    protected void assertYdbTableMatches(ExpectedYdbTable expected, TableDescription actual) {
        if (expected == null || actual == null) {
            throw new IllegalArgumentException("Expected and actual must be non-null");
        }

        // 1) Check that all expected columns exist with correct type and nullability
        for (ExpectedYdbColumn ec : expected.getColumns()) {
            TableColumn col = actual.getColumns().stream()
                    .filter(c -> c.getName().equals(ec.getName()))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Missing column " + ec.getName()
                            + " in table " + expected.getFullName()));

            Type type = col.getType();
            boolean isOptional = Type.Kind.OPTIONAL.equals(type.getKind());
            if (ec.isNullable() != isOptional) {
                throw new AssertionError("Nullability mismatch for column " + ec.getName()
                        + " in table " + expected.getFullName());
            }

            Type bareType = isOptional ? type.unwrapOptional() : type;
            if (!bareType.equals(ec.getType())) {
                throw new AssertionError("Type mismatch for column " + ec.getName()
                        + " in table " + expected.getFullName()
                        + ": expected " + ec.getType()
                        + ", actual " + bareType);
            }
        }

        // 2) Ensure there are no unexpected extra columns
        Set<String> expectedNames = new HashSet<>();
        for (ExpectedYdbColumn ec : expected.getColumns()) {
            expectedNames.add(ec.getName());
        }
        for (TableColumn col : actual.getColumns()) {
            if (!expectedNames.contains(col.getName())) {
                throw new AssertionError("Unexpected column " + col.getName()
                        + " in table " + expected.getFullName());
            }
        }

        // 3) Ensure primary key from YDB matches expected primary key exactly (order
        // and names)
        List<String> actualPk = actual.getPrimaryKeys();
        List<String> expectedPk = expected.getPrimaryKey();
        if (!actualPk.equals(expectedPk)) {
            throw new AssertionError("Primary key mismatch for table " + expected.getFullName()
                    + ": expected " + expectedPk
                    + ", actual " + actualPk);
        }
    }

    /**
     * Data assertions for a single table based on ExpectedRow descriptors.
     */
    protected void assertYdbDataMatches(ExpectedYdbTable expected, List<RowData> actualRows) {
        List<ExpectedRow> expectedRows = expected.getExpectedRows();
        if (expectedRows == null || expectedRows.isEmpty()) {
            if (actualRows != null && !actualRows.isEmpty()) {
                throw new AssertionError(String.format(
                        "Expected no rows in table %s, but YDB returned %d row(s)",
                        expected.getFullName(),
                        actualRows.size()));
            }
            return;
        }
        if (actualRows == null || actualRows.isEmpty()) {
            throw new AssertionError(String.format(
                    "Expected %d row(s) in table %s, but YDB returned none",
                    expectedRows.size(),
                    expected.getFullName()));
        }

        assertData(expected, actualRows);
    }

    private void configureWorkers(ImporterConfig config) {
        WorkerConfig workers = new WorkerConfig();
        workers.setPoolSize(WORKER_POOL_SIZE);
        config.setWorkers(workers);
    }

    private void configureSource(
            ImporterConfig config,
            ImportDialect dialect,
            JdbcDatabaseContainer<?> source) {
        SourceConfig src = new SourceConfig();
        src.setType(dialect.sourceType());
        src.setClassName(dialect.getJdbcDriverClass());
        src.setJdbcUrl(source.getJdbcUrl());
        src.setUserName(source.getUsername());
        src.setPassword(source.getPassword());
        config.setSource(src);
    }

    private void configureTarget(ImporterConfig config, ImportCase testCase) {
        TargetConfig tgt = new TargetConfig();
        tgt.setType(TargetType.YDB);
        tgt.setAuthMode(YdbAuthMode.NONE);
        tgt.setConnectionString(getYdbContainer().getConnectionString());
        tgt.setReplaceExisting(true);
        tgt.setLoadData(testCase.isLoadData());
        tgt.setMaxBatchRows(MAX_BATCH_ROWS);
        tgt.setMaxBlobRows(MAX_BLOB_ROWS);
        config.setTarget(tgt);
    }

    private void configureTableOptions(ImporterConfig config, ImportDialect dialect, ImportCase testCase) {
        if (testCase.getTableOptions().isEmpty()) {
            createDefaultTableOptions(config, dialect);
        } else {
            createCustomTableOptions(config, testCase);
        }
    }

    private void createDefaultTableOptions(ImporterConfig config, ImportDialect dialect) {
        String prefix = dialect.name();
        TableOptions options = new TableOptions(DEFAULT_TABLE_OPTIONS_NAME, prefix + ".${schema}.${table}");
        options.setBlobTemplate(prefix + ".${schema}.${table}_${field}");
        config.getOptionsMap().put(options.getName(), options);
    }

    private void createCustomTableOptions(ImporterConfig config, ImportCase testCase) {
        for (TableOptionsConfig toc : testCase.getTableOptions()) {
            TableOptions options = createTableOptionsFromConfig(toc);
            config.getOptionsMap().put(options.getName(), options);
        }
    }

    private TableOptions createTableOptionsFromConfig(TableOptionsConfig toc) {
        TableOptions options = new TableOptions(toc.getName(), toc.getTableNameFormat());
        options.setBlobTemplate(toc.getBlobNameFormat());
        options.setAllowCustomDecimal(toc.isAllowCustomDecimal());
        options.setSkipUnknownTypes(toc.isSkipUnknownTypes());
        options.setCaseMode(toc.getCaseMode());
        options.setDateConv(toc.getConvDate());
        options.setTimestampConv(toc.getConvTimestamp());
        return options;
    }

    private void configureTableRefs(ImporterConfig config, ImportCase testCase) {
        for (SourceTableRef ref : testCase.getTables()) {
            TableRef tableRef = createTableRef(config, ref);
            config.getTableRefs().add(tableRef);
        }
    }

    private TableRef createTableRef(ImporterConfig config, SourceTableRef ref) {
        TableRef tableRef = new TableRef();
        tableRef.setOptions(getTableOptions(config, ref.getTableOptionsName()));
        tableRef.setSchema(ref.getSchemaName());
        tableRef.setTable(ref.getTableName());
        tableRef.setQueryText(ref.getQueryText());
        tableRef.getKeyNames().addAll(ref.getKeyNames());
        return tableRef;
    }

    private TableOptions getTableOptions(ImporterConfig config, String optionsName) {
        String actualName = (optionsName == null || optionsName.isEmpty())
                ? DEFAULT_TABLE_OPTIONS_NAME
                : optionsName;

        TableOptions options = config.getOptionsMap().get(actualName);
        if (options == null) {
            throw new IllegalStateException("Missing TableOptions with name " + actualName);
        }
        return options;
    }

    private void assertData(ExpectedYdbTable expected, List<RowData> actualRows) {
        List<RowData> remainingRows = new ArrayList<>(actualRows);

        for (ExpectedRow expectedRow : expected.getExpectedRows()) {
            RowData matchingRow = findMatchingRow(expectedRow, remainingRows);

            if (matchingRow == null) {
                throw new AssertionError(String.format(
                        "Missing row matching %s in table %s",
                        expectedRow.getValues(),
                        expected.getFullName()));
            }

            remainingRows.remove(matchingRow);
        }

        if (!remainingRows.isEmpty()) {
            throw new AssertionError(String.format(
                    "Found %d unexpected extra row(s) in table %s",
                    remainingRows.size(),
                    expected.getFullName()));
        }
    }

    private RowData findMatchingRow(ExpectedRow expectedRow, List<RowData> rows) {
        for (RowData row : rows) {
            if (rowsMatch(expectedRow, row)) {
                return row;
            }
        }
        return null;
    }

    private boolean rowsMatch(ExpectedRow expectedRow, RowData actualRow) {
        for (Map.Entry<String, Value<?>> entry : expectedRow.getValues().entrySet()) {
            String columnName = entry.getKey();
            Value<?> expectedValue = entry.getValue();
            Value<?> actualValue = actualRow.getValues().get(columnName);

            if (!expectedValue.equals(actualValue)) {
                return false;
            }
        }
        return true;
    }
}