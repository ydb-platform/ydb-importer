package tech.ydb.importer.integration.sources.oracle;

import tech.ydb.importer.integration.verification.AbstractDialectLoader;
import tech.ydb.importer.integration.verification.LogicalType;
import tech.ydb.importer.integration.verification.PartitionStyle;
import tech.ydb.importer.integration.verification.TableScenario;

public final class OracleLoader extends AbstractDialectLoader {

    public static final OracleLoader INSTANCE = new OracleLoader();

    private OracleLoader() {
    }

    @Override
    protected String toDdl(LogicalType type) {
        switch (type) {
            case INT32:           return "NUMBER(10)";
            case INT64:           return "NUMBER(19)";
            case DECIMAL_18_4:    return "NUMBER(18, 4)";
            case STRING:          return "VARCHAR2(255)";
            case BOOL:            return "BOOLEAN";
            case DATE:            return "DATE";
            case DATETIME:        return "TIMESTAMP";
            case NULLABLE_STRING: return "VARCHAR2(255)";
            default:
                throw new IllegalArgumentException("Unsupported: " + type);
        }
    }

    @Override
    protected String blobDdlType() {
        return "BLOB";
    }

    @Override
    protected void appendPartition(StringBuilder ddl, PartitionStyle style,
                                   String column, TableScenario scenario) {
        switch (style) {
            case HASH_INT:
                ddl.append(" PARTITION BY HASH(").append(column)
                        .append(") PARTITIONS ")
                        .append(partitionCount(scenario));
                break;
            case RANGE_INT: {
                long[] b = rangeIntBoundaries(scenario);
                ddl.append(" PARTITION BY RANGE(").append(column).append(") (");
                for (int i = 0; i < b.length; i++) {
                    ddl.append("PARTITION p").append(i)
                            .append(" VALUES LESS THAN (").append(b[i])
                            .append("),");
                }
                ddl.append("PARTITION pmax VALUES LESS THAN (MAXVALUE))");
                break;
            }
            case RANGE_DATE:
                ddl.append(" PARTITION BY RANGE(").append(column).append(") (")
                        .append("PARTITION p2024 VALUES LESS THAN ")
                        .append("(TIMESTAMP '2025-01-01 00:00:00'),")
                        .append("PARTITION p2025 VALUES LESS THAN ")
                        .append("(TIMESTAMP '2026-01-01 00:00:00'),")
                        .append("PARTITION p2026 VALUES LESS THAN ")
                        .append("(TIMESTAMP '2027-01-01 00:00:00'),")
                        .append("PARTITION pmax VALUES LESS THAN (MAXVALUE))");
                break;
            case LIST_STRING:
                ddl.append(" PARTITION BY LIST(").append(column).append(") (")
                        .append("PARTITION p_card VALUES ('card'),")
                        .append("PARTITION p_cash VALUES ('cash'),")
                        .append("PARTITION p_transfer VALUES ('transfer'),")
                        .append("PARTITION p_crypto VALUES ('crypto'))");
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
}
