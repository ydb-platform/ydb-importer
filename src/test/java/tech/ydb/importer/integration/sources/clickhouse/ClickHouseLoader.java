package tech.ydb.importer.integration.sources.clickhouse;

import tech.ydb.importer.integration.verification.ColumnSpec;
import tech.ydb.importer.integration.verification.DialectLoader;
import tech.ydb.importer.integration.verification.LogicalType;
import tech.ydb.importer.integration.verification.TableScenario;
import tech.ydb.importer.integration.verification.TableScenario.PartitionStyle;

public final class ClickHouseLoader extends DialectLoader {

    public static final ClickHouseLoader INSTANCE = new ClickHouseLoader();

    private ClickHouseLoader() {
    }

    @Override
    protected String toDdl(LogicalType type) {
        switch (type) {
            case INT32:           return "Int32";
            case INT64:           return "Int64";
            case DECIMAL_18_4:    return "Decimal(18, 4)";
            case STRING:          return "String";
            case BOOL:            return "Bool";
            case DATE:            return "Date";
            case DATETIME:        return "DateTime";
            default:
                throw new IllegalArgumentException("Unsupported: " + type);
        }
    }

    @Override
    protected String columnDdl(ColumnSpec col) {
        String base = toDdl(col.type());
        return col.nullable() ? "Nullable(" + base + ")" : base;
    }

    @Override
    protected void appendEngine(StringBuilder ddl, TableScenario scenario) {
        ddl.append(" ENGINE = MergeTree() ORDER BY ")
                .append(scenario.keyColumn());
    }

    @Override
    protected void appendPartition(StringBuilder ddl, PartitionStyle style,
                                   String column, TableScenario scenario) {
        int p = partitionCount(scenario);
        String expr;
        switch (style) {
            case HASH_INT:    expr = "sipHash64(" + column + ") % " + p; break;
            case RANGE_INT:   expr = column + " % " + p; break;
            case RANGE_DATE:  expr = "toYear(" + column + ") % " + p; break;
            case LIST_STRING: expr = column; break;
            default:
                throw new IllegalArgumentException("Unsupported: " + style);
        }
        ddl.append(" PARTITION BY ").append(expr);
    }
}
