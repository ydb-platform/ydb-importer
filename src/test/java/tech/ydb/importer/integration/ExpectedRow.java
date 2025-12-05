package tech.ydb.importer.integration;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import tech.ydb.table.values.Value;

/**
 * Expected row data for a YDB table.
 */
public final class ExpectedRow {

    private final Map<String, Value<?>> values;

    public ExpectedRow(Map<String, Value<?>> values) {
        this.values = Collections.unmodifiableMap(values);
    }

    public Map<String, Value<?>> getValues() {
        return values;
    }

    /**
     * Convenience factory for building a row from key/value pairs:
     * ExpectedRow.of("id", v1, "name", v2, ...).
     */
    public static ExpectedRow of(Object... keyValues) {
        if (keyValues == null || (keyValues.length % 2) != 0) {
            throw new IllegalArgumentException("Expected even number of key/value arguments");
        }
        Map<String, Value<?>> map = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            Object k = keyValues[i];
            if (!(k instanceof String)) {
                throw new IllegalArgumentException("Key at index " + i + " is not a String");
            }
            String key = (String) k;
            Object value = keyValues[i + 1];
            if (value != null && !(value instanceof Value)) {
                throw new IllegalArgumentException(
                        "Value for key '" + key + "' is not a YDB Value<?> instance"
                );
            }
            map.put(key, (Value<?>) value);
        }
        return new ExpectedRow(map);
    }
}


