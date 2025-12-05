package tech.ydb.importer.integration;

import java.util.Collections;
import java.util.List;

/**
 * Description of a single end-to-end import scenario.
 * Defines source tables, importer options and expected YDB result.
 */
public final class ImportCase {

    private final String id;
    private final String description;
    private final boolean loadData;
    private final List<TableOptionsConfig> tableOptions;
    private final List<SourceTableRef> tables;
    private final List<ExpectedYdbTable> expectedTables;

    public ImportCase(
            String id,
            String description,
            boolean loadData,
            List<TableOptionsConfig> tableOptions,
            List<SourceTableRef> tables,
            List<ExpectedYdbTable> expectedTables
    ) {
        this.id = id;
        this.description = description;
        this.loadData = loadData;
        this.tableOptions = Collections.unmodifiableList(tableOptions);
        this.tables = Collections.unmodifiableList(tables);
        this.expectedTables = Collections.unmodifiableList(expectedTables);
    }

    public String getId() {
        return id;
    }

    public String getDescription() {
        return description;
    }

    public boolean isLoadData() {
        return loadData;
    }

    public List<TableOptionsConfig> getTableOptions() {
        return tableOptions;
    }

    public List<SourceTableRef> getTables() {
        return tables;
    }

    public List<ExpectedYdbTable> getExpectedTables() {
        return expectedTables;
    }
}


