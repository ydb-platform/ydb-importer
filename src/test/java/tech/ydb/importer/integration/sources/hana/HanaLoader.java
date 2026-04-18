package tech.ydb.importer.integration.sources.hana;

import java.util.EnumSet;
import java.util.Set;

import tech.ydb.importer.integration.verification.AbstractDialectLoader;
import tech.ydb.importer.integration.verification.LogicalType;
import tech.ydb.importer.integration.verification.PartitionStyle;
import tech.ydb.importer.integration.verification.TableScenario;

public final class HanaLoader extends AbstractDialectLoader {

    public static final HanaLoader INSTANCE = new HanaLoader();

    private HanaLoader() {
    }

    @Override
    protected String toDdl(LogicalType type) {
        switch (type) {
            case INT32:           return "INTEGER";
            case INT64:           return "BIGINT";
            case DECIMAL_18_4:    return "DECIMAL(18, 4)";
            case STRING:          return "NVARCHAR(255)";
            case BOOL:            return "BOOLEAN";
            case DATE:            return "DATE";
            case DATETIME:        return "TIMESTAMP";
            case NULLABLE_STRING: return "NVARCHAR(255)";
            default:
                throw new IllegalArgumentException("Unsupported: " + type);
        }
    }

    @Override
    protected String blobDdlType() {
        return "BLOB";
    }

    @Override
    public Set<PartitionStyle> supportedPartitions() {
        return EnumSet.of(PartitionStyle.HASH_INT, PartitionStyle.RANGE_INT,
                PartitionStyle.RANGE_DATE);
    }

    @Override
    protected void appendPartition(StringBuilder ddl, PartitionStyle style,
                                   String column, TableScenario scenario) {
        switch (style) {
            case HASH_INT:
            case RANGE_INT:
                ddl.append(" PARTITION BY HASH(").append(column)
                        .append(") PARTITIONS ")
                        .append(partitionCount(scenario));
                break;
            case RANGE_DATE:
                ddl.append(" PARTITION BY RANGE(").append(column).append(")")
                        .append(" (PARTITION '2025-01-01' <= VALUES < '2026-01-01',")
                        .append(" PARTITION '2026-01-01' <= VALUES < '2027-01-01',")
                        .append(" PARTITION OTHERS)");
                break;
            default:
                break;
        }
    }

    @Override
    protected String createTablePrefix() {
        return "CREATE TABLE ";
    }

    @Override
    public String effectiveTableName(String name) {
        return name.toUpperCase();
    }

    @Override
    public boolean requiresSetSchema() {
        return true;
    }

    @Override
    public boolean supportsMultiRowInsert() {
        return false;
    }
}
