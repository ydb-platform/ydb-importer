package tech.ydb.importer.integration.verification;

public final class ColumnSpec {

    private final String name;
    private final LogicalType type;

    public ColumnSpec(String name, LogicalType type) {
        this.name = name;
        this.type = type;
    }

    public String name() {
        return name;
    }

    public LogicalType type() {
        return type;
    }
}
