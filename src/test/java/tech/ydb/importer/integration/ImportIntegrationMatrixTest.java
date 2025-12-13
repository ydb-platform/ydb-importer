package tech.ydb.importer.integration;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.testcontainers.containers.JdbcDatabaseContainer;

import tech.ydb.importer.config.ImporterConfig;
import tech.ydb.importer.integration.dialects.ClickHouseImportDialect;
import tech.ydb.importer.integration.dialects.PostgresImportDialect;
import tech.ydb.table.description.TableDescription;

/**
 * Import tests for JDBC sources into YDB.
 */
public class ImportIntegrationMatrixTest extends BaseImportIntegrationTest {

    @Override
    protected List<ImportDialect> dialects() {
        return Arrays.asList(
                new PostgresImportDialect(),
                new ClickHouseImportDialect()
        );
    }

    @TestFactory
    Stream<DynamicTest> importIntegrationMatrixTest() {
        return dialects().stream()
                .map(dialect -> DynamicTest.dynamicTest(dialect.name(), () -> runDialect(dialect)));
    }

    private void runDialect(ImportDialect dialect) throws Exception {
        JdbcDatabaseContainer<?> source = dialect.createContainer();
        source.start();
        try {
            for (DialectCase dialectCase : dialect.cases()) {
                ImportCase testCase = dialectCase.getImportCase();
                try {
                    runSingleCase(dialect, dialectCase, source);
                } catch (Exception e) {
                    throw new AssertionError(dialect.name() + "/" + testCase.getId() + " failed: "
                            + testCase.getDescription(), e);
                }
            }
        } finally {
            source.stop();
        }
    }

    private void runSingleCase(ImportDialect dialect, DialectCase dialectCase, JdbcDatabaseContainer<?> source)
            throws Exception {
        ImportCase testCase = dialectCase.getImportCase();

        try (Connection con = openSourceConnection(source)) {
            executeSql(con, dialectCase.prepareSourceSql());
        }

        try {
            ImporterConfig config = buildImporterConfig(dialect, testCase, source);
            runImporter(config);

            try (TestYdbClient ydbClient = createYdbClient()) {
                for (ExpectedYdbTable expected : testCase.getExpectedTables()) {
                    TableDescription desc = ydbClient.describeLogicalTable(expected.getFullName());
                    assertYdbTableMatches(expected, desc);
                    List<RowData> rows = ydbClient.readAll(expected);
                    assertYdbDataMatches(expected, rows);
                }
            }
        } finally {
            try (Connection con = openSourceConnection(source)) {
                executeSql(con, dialectCase.cleanupSourceSql());
            }
        }
    }

    private static void executeSql(Connection con, List<String> statements) throws Exception {
        try (Statement st = con.createStatement()) {
            for (String sql : statements) {
                st.execute(sql);
            }
        }
    }
}
