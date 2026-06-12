package tech.ydb.importer.integration.verification;

public final class ColumnSpec {

    private final String name;
    private final LogicalType type;
    private final boolean nullable;

    public ColumnSpec(String name, LogicalType type) {
        this(name, type, false);
    }

    public ColumnSpec(String name, LogicalType type, boolean nullable) {
        this.name = name;
        this.type = type;
        this.nullable = nullable;
    }

    public String name() {
        return name;
    }

    public LogicalType type() {
        return type;
    }

    public boolean nullable() {
        return nullable;
    }
}
