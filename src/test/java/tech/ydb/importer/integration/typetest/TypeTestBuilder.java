package tech.ydb.importer.integration.typetest;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

import tech.ydb.importer.config.TableOptions;
import tech.ydb.importer.integration.common.YdbImporterRunner;
import tech.ydb.importer.integration.common.YdbRowMatcher;
import tech.ydb.importer.integration.common.YdbSchemaReader;
import tech.ydb.importer.integration.common.YdbSchemaReader.YdbTableInfo;
import tech.ydb.table.values.Type;

/**
 * Nested fluent builder for type-mapping round-trip tests.
 */
public final class TypeTestBuilder {

    private final AbstractYdbImporterTypeTest test;
    private final List<TypeCase> completed = new ArrayList<>();
    private Consumer<TableOptions> optionsCustomizer = opts -> { };
    private boolean useArrow;
    private String identifierQuote = "";

    private String currentSourceType;
    private Type currentExpectedYdbType;
    private List<TypeCase.ValueCase> currentValues;

    TypeTestBuilder(AbstractYdbImporterTypeTest test) {
        this.test = test;
    }

    public TypeTestBuilder withArrow(boolean value) {
        this.useArrow = value;
        return this;
    }

    /**
     * Quote identifiers for DBs that fold unquoted names (HANA, Oracle, Db2).
     */
    public TypeTestBuilder withIdentifierQuote(String quote) {
        this.identifierQuote = quote == null ? "" : quote;
        return this;
    }

    public TypeTestBuilder withOptions(Consumer<TableOptions> customizer) {
        this.optionsCustomizer = customizer == null ? opts -> { } : customizer;
        return this;
    }

    public TypeTestBuilder column(String sourceType, Type expectedYdbType) {
        flushCurrent();
        this.currentSourceType = sourceType;
        this.currentExpectedYdbType = expectedYdbType;
        this.currentValues = new ArrayList<>();
        return this;
    }

    public TypeTestBuilder value(String insertLiteral, Object expectedValue) {
        if (currentValues == null) {
            throw new IllegalStateException(
                    ".value() requires a prior .column() declaration");
        }
        currentValues.add(new TypeCase.ValueCase(insertLiteral, expectedValue));
        return this;
    }

    public void execute() throws Exception {
        flushCurrent();
        if (completed.isEmpty()) {
            throw new IllegalStateException(
                    "typeTest() requires at least one .column(...).value(...)");
        }

        String tableName = "typetest_" + Long.toHexString(
                ThreadLocalRandom.current().nextLong() & 0xFFFFFFFFL);
        String targetPath = YdbImporterRunner.DEFAULT_TARGET_PREFIX
                + "." + test.schemaName() + "." + tableName;
        String setupDdl = renderCreateTable(tableName);
        List<String> insertStatements = renderInserts(tableName);

        try (YdbSchemaReader ydb = new YdbSchemaReader(
                test.ydbContainer().getConnectionString())) {
            try {
                executeSetup(setupDdl, insertStatements);
                runImporter(tableName);
                verifyResults(ydb, targetPath);
            } finally {
                cleanupSource(tableName);
                ydb.dropTable(targetPath);
            }
        }
    }

    private void flushCurrent() {
        if (currentValues == null) {
            return;
        }
        String columnName = "col_" + completed.size();
        completed.add(new TypeCase(columnName, currentSourceType,
                currentExpectedYdbType, currentValues));
        currentSourceType = null;
        currentExpectedYdbType = null;
        currentValues = null;
    }

    private String quoteId(String id) {
        return identifierQuote + id + identifierQuote;
    }

    private String renderCreateTable(String tableName) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(quoteId(test.schemaName()))
                .append(".").append(quoteId(tableName));
        sb.append(" (");
        boolean first = true;
        for (TypeCase tc : completed) {
            if (!first) {
                sb.append(", ");
            }
            first = false;
            sb.append(quoteId(tc.getColumnName()))
                    .append(' ').append(tc.getSourceType());
        }
        sb.append(")");
        sb.append(test.createTableSuffix());
        return sb.toString();
    }

    private List<String> renderInserts(String tableName) {
        int maxRows = 0;
        for (TypeCase tc : completed) {
            if (tc.getValues().size() > maxRows) {
                maxRows = tc.getValues().size();
            }
        }

        List<String> statements = new ArrayList<>(maxRows);
        for (int row = 0; row < maxRows; row++) {
            StringBuilder sb = new StringBuilder();
            sb.append("INSERT INTO ").append(quoteId(test.schemaName()))
                    .append(".").append(quoteId(tableName));
            sb.append(" (");
            boolean first = true;
            for (TypeCase tc : completed) {
                if (!first) {
                    sb.append(", ");
                }
                first = false;
                sb.append(quoteId(tc.getColumnName()));
            }
            sb.append(") VALUES (");
            first = true;
            for (TypeCase tc : completed) {
                if (!first) {
                    sb.append(", ");
                }
                first = false;
                String literal = row < tc.getValues().size()
                        ? tc.getValues().get(row).getInsertLiteral()
                        : "NULL";
                sb.append(literal);
            }
            sb.append(")");
            statements.add(sb.toString());
        }
        return statements;
    }

    private void executeSetup(String ddl, List<String> inserts)
            throws Exception {
        try (Connection con = test.openSourceConnection()) {
            try (Statement st = con.createStatement()) {
                st.execute(ddl);
            }
            for (String insert : inserts) {
                try (Statement st = con.createStatement()) {
                    st.execute(insert);
                }
            }
        }
    }

    private void runImporter(String tableName) throws Exception {
        YdbImporterRunner.builder()
                .source(test.sourceContainer(), test.sourceType())
                .ydb(test.ydbContainer())
                .table(test.schemaName(), tableName)
                .customizeOptions(optionsCustomizer)
                .useArrow(useArrow)
                .run();
    }

    private void verifyResults(YdbSchemaReader ydb, String targetPath) {
        YdbTableInfo info = ydb.describe(targetPath);
        verifyColumnTypes(info);

        List<Map<String, Object>> rows = ydb.readRows(targetPath,
                completed.get(0).getColumnName());
        verifyColumnValues(rows);
    }

    private void verifyColumnTypes(YdbTableInfo info) {
        for (TypeCase tc : completed) {
            assertEquals(tc.getExpectedYdbType(),
                    info.getColumn(tc.getColumnName()).getRawType(),
                    "YDB type for " + tc.getColumnName()
                    + " (source: " + tc.getSourceType() + ")");
        }
    }

    private void verifyColumnValues(List<Map<String, Object>> rows) {
        int maxExpected = 0;
        for (TypeCase tc : completed) {
            if (tc.getValues().size() > maxExpected) {
                maxExpected = tc.getValues().size();
            }
        }
        assertEquals(maxExpected, rows.size(), "Row count");

        for (TypeCase tc : completed) {
            List<TypeCase.ValueCase> expectedValues = tc.getValues();
            for (int i = 0; i < expectedValues.size(); i++) {
                YdbRowMatcher.assertRowExists(rows,
                        tc.getColumnName(),
                        expectedValues.get(i).getExpectedValue());
            }
        }
    }

    private void cleanupSource(String tableName) {
        try (Connection con = test.openSourceConnection();
             Statement st = con.createStatement()) {
            st.execute("DROP TABLE IF EXISTS "
                    + quoteId(test.schemaName()) + "." + quoteId(tableName));
        } catch (Exception ignored) {
        }
    }
}
