package tech.ydb.importer.integration;

import tech.ydb.table.values.Type;

/**
 * Expected YDB column definition.
 */
public final class ExpectedYdbColumn {

    private final String name;
    private final Type type;
    private final boolean nullable;

    public ExpectedYdbColumn(String name, Type type, boolean nullable) {
        this.name = name;
        this.type = type;
        this.nullable = nullable;
    }

    public String getName() {
        return name;
    }

    public Type getType() {
        return type;
    }

    public boolean isNullable() {
        return nullable;
    }
}
