package tech.ydb.importer.integration;

import java.util.Collections;
import java.util.List;

/**
 * Source table reference description.
 */
public final class SourceTableRef {

    private final String schemaName;
    private final String tableName;
    private final List<String> keyNames;
    private final String queryText;
    private final String tableOptionsName;

    public SourceTableRef(
            String schemaName,
            String tableName,
            List<String> keyNames,
            String queryText,
            String tableOptionsName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.keyNames = keyNames == null ? Collections.emptyList() : Collections.unmodifiableList(keyNames);
        this.queryText = queryText;
        this.tableOptionsName = tableOptionsName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getKeyNames() {
        return keyNames;
    }

    public String getQueryText() {
        return queryText;
    }

    public String getTableOptionsName() {
        return tableOptionsName;
    }
}
