package tech.ydb.importer.integration.sources.db2;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.EnumSet;
import java.util.Set;

import tech.ydb.importer.integration.verification.DialectLoader;
import tech.ydb.importer.integration.verification.LogicalType;
import tech.ydb.importer.integration.verification.TableScenario.PartitionStyle;
import tech.ydb.importer.integration.verification.TableScenario;

public final class Db2Loader extends DialectLoader {

    public static final Db2Loader INSTANCE = new Db2Loader();

    private Db2Loader() {
    }

    @Override
    protected String toDdl(LogicalType type) {
        switch (type) {
            case INT32:           return "INTEGER";
            case INT64:           return "BIGINT";
            case DECIMAL_18_4:    return "DECIMAL(18, 4)";
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
        return "BLOB";
    }

    @Override
    public Set<PartitionStyle> supportedPartitions() {
        return EnumSet.of(PartitionStyle.RANGE_INT, PartitionStyle.RANGE_DATE);
    }

    @Override
    public int loaderPoolSize() {
        return 1;
    }

    @Override
    protected int partitionCount(TableScenario scenario) {
        return 4;
    }

    @Override
    protected void appendPartition(StringBuilder ddl, PartitionStyle style,
                                   String column, TableScenario scenario) {
        switch (style) {
            case RANGE_INT: {
                long[] b = rangeIntBoundaries(scenario);
                ddl.append(" PARTITION BY RANGE(").append(column).append(") (");
                for (int i = 0; i < b.length; i++) {
                    if (i == 0) {
                        ddl.append("PARTITION p0 STARTING FROM (0) ");
                    } else {
                        ddl.append("PARTITION p").append(i).append(' ');
                    }
                    ddl.append("ENDING AT (").append(b[i]).append("),");
                }
                ddl.append("PARTITION pmax ENDING AT (MAXVALUE))");
                break;
            }
            case RANGE_DATE:
                ddl.append(" PARTITION BY RANGE(").append(column).append(")")
                        .append(" (PARTITION p2024 STARTING FROM")
                        .append(" ('2024-01-01') ENDING AT ('2025-01-01'),")
                        .append(" PARTITION p2025 ENDING AT ('2026-01-01'),")
                        .append(" PARTITION p2026 ENDING AT ('2027-01-01'),")
                        .append(" PARTITION pmax ENDING AT (MAXVALUE))");
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
    public String adaptTableName(String name) {
        return name.toUpperCase();
    }

    @Override
    public void onConnectionOpened(Connection conn, String schema) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("SET CURRENT SCHEMA " + schema);
        }
    }
}
