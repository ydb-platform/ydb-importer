package tech.ydb.importer.integration;

import java.util.Collections;
import java.util.List;

/**
 * Expected YDB table definition (schema + optional row expectations).
 */
public final class ExpectedYdbTable {

    private final String fullName;
    private final List<ExpectedYdbColumn> columns;
    private final List<String> primaryKey;
    private final List<ExpectedRow> expectedRows;

    public ExpectedYdbTable(
            String fullName,
            List<ExpectedYdbColumn> columns,
            List<String> primaryKey
    ) {
        this(fullName, columns, primaryKey, Collections.emptyList());
    }

    public ExpectedYdbTable(
            String fullName,
            List<ExpectedYdbColumn> columns,
            List<String> primaryKey,
            List<ExpectedRow> expectedRows
    ) {
        this.fullName = fullName;
        this.columns = Collections.unmodifiableList(columns);
        this.primaryKey = Collections.unmodifiableList(primaryKey);
        this.expectedRows = Collections.unmodifiableList(expectedRows);
    }

    public String getFullName() {
        return fullName;
    }

    public List<ExpectedYdbColumn> getColumns() {
        return columns;
    }

    public List<String> getPrimaryKey() {
        return primaryKey;
    }

    public List<ExpectedRow> getExpectedRows() {
        return expectedRows;
    }
}


