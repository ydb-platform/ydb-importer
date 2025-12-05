package tech.ydb.importer.integration;

import java.util.Collections;
import java.util.Map;

import tech.ydb.table.values.Value;

/**
 * Actual row data fetched from YDB for assertions.
 */
public final class RowData {

    private final Map<String, Value<?>> values;

    public RowData(Map<String, Value<?>> values) {
        this.values = Collections.unmodifiableMap(values);
    }

    public Map<String, Value<?>> getValues() {
        return values;
    }
}


