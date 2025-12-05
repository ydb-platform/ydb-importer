package tech.ydb.importer.integration;

import java.sql.Connection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.testcontainers.containers.JdbcDatabaseContainer;

import tech.ydb.importer.config.ImporterConfig;
import tech.ydb.table.description.TableDescription;

/**
 * Import tests for JDBC sources into YDB.
 */
public class ImportIntegrationMatrixTest extends BaseImportIntegrationTest {

    @Override
    protected List<ImportDialect> dialects() {
        return Collections.singletonList(new PostgresImportDialect());
    }

    @TestFactory
    Stream<DynamicTest> importIntegrationMatrixTest() {
        return dialects().stream()
                .flatMap(dialect -> dialect.cases().stream().map(dialectCase -> {
                    ImportCase testCase = dialectCase.getImportCase();
                    String displayName = dialect.name() + "/" + testCase.getId();
                    return DynamicTest.dynamicTest(displayName,
                            () -> runSingleCase(dialect, dialectCase));
                }));
    }

    private void runSingleCase(ImportDialect dialect, DialectCase dialectCase) throws Exception {
        JdbcDatabaseContainer<?> source = dialect.createContainer();
        source.start();
        try {
            ImportCase testCase = dialectCase.getImportCase();

            try (Connection con = openSourceConnection(source)) {
                dialectCase.prepareSourceData(con);
            }

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
            source.stop();
        }
    }
}
