package tech.ydb.importer.integration.sources.vertica;

import java.util.EnumSet;
import java.util.Set;

import tech.ydb.importer.integration.verification.DialectLoader;
import tech.ydb.importer.integration.verification.LogicalType;
import tech.ydb.importer.integration.verification.TableScenario.PartitionStyle;
import tech.ydb.importer.integration.verification.TableScenario;

public final class VerticaLoader extends DialectLoader {

    public static final VerticaLoader INSTANCE = new VerticaLoader();

    private VerticaLoader() {
    }

    @Override
    protected String toDdl(LogicalType type) {
        switch (type) {
            case INT32:           return "INTEGER";
            case INT64:           return "BIGINT";
            case DECIMAL_18_4:    return "NUMERIC(18, 4)";
            case STRING:          return "VARCHAR(255)";
            case BOOL:            return "BOOLEAN";
            case DATE:            return "DATE";
            case DATETIME:        return "TIMESTAMP";
            default:
                throw new IllegalArgumentException("Unsupported: " + type);
        }
    }

    @Override
    protected String blobDdlType() {
        return "LONG VARBINARY";
    }

    @Override
    public boolean supportsMultiRowInsert() {
        return false;
    }

    @Override
    public Set<PartitionStyle> supportedPartitions() {
        return EnumSet.of(PartitionStyle.HASH_INT, PartitionStyle.RANGE_INT,
                PartitionStyle.RANGE_DATE);
    }

    @Override
    protected void appendPartition(StringBuilder ddl, PartitionStyle style,
                                   String column, TableScenario scenario) {
        int p = partitionCount(scenario);
        switch (style) {
            case HASH_INT:
            case RANGE_INT:
                ddl.append(" PARTITION BY ").append(column)
                        .append(" % ").append(p);
                break;
            case RANGE_DATE:
                ddl.append(" PARTITION BY EXTRACT(YEAR FROM ")
                        .append(column).append(") % ").append(p);
                break;
            default:
                break;
        }
    }
}
