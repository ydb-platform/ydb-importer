package tech.ydb.importer.integration.sources.mariadb;

import tech.ydb.importer.integration.verification.DialectLoader;
import tech.ydb.importer.integration.verification.LogicalType;
import tech.ydb.importer.integration.verification.TableScenario.PartitionStyle;
import tech.ydb.importer.integration.verification.TableScenario;

public class MariaDbLoader extends DialectLoader {

    public static final MariaDbLoader INSTANCE = new MariaDbLoader();

    protected MariaDbLoader() {
    }

    @Override
    protected String toDdl(LogicalType type) {
        switch (type) {
            case INT32:           return "INT";
            case INT64:           return "BIGINT";
            case DECIMAL_18_4:    return "DECIMAL(18, 4)";
            case STRING:          return "VARCHAR(255)";
            case BOOL:            return "BOOLEAN";
            case DATE:            return "DATE";
            case DATETIME:        return "DATETIME";
            default:
                throw new IllegalArgumentException("Unsupported: " + type);
        }
    }

    @Override
    protected String blobDdlType() {
        return "MEDIUMBLOB";
    }

    @Override
    protected void appendEngine(StringBuilder ddl, TableScenario scenario) {
        ddl.append(" ENGINE=InnoDB");
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
                ddl.append("PARTITION pmax VALUES LESS THAN MAXVALUE)");
                break;
            }
            case RANGE_DATE:
                ddl.append(" PARTITION BY RANGE(YEAR(").append(column)
                        .append(")) (")
                        .append("PARTITION p2024 VALUES LESS THAN (2025),")
                        .append("PARTITION p2025 VALUES LESS THAN (2026),")
                        .append("PARTITION p2026 VALUES LESS THAN (2027),")
                        .append("PARTITION pmax VALUES LESS THAN MAXVALUE)");
                break;
            case LIST_STRING:
                ddl.append(" PARTITION BY LIST COLUMNS(").append(column)
                        .append(") (")
                        .append("PARTITION p_card VALUES IN ('card'),")
                        .append("PARTITION p_cash VALUES IN ('cash'),")
                        .append("PARTITION p_transfer VALUES IN ('transfer'),")
                        .append("PARTITION p_crypto VALUES IN ('crypto'))");
                break;
            default:
                break;
        }
    }
}
