package tech.ydb.importer.integration.verification;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public abstract class AbstractDialectLoader implements DialectLoader {

    @Override
    public void createTable(Connection conn, TableScenario scenario)
            throws SQLException {
        StringBuilder ddl = new StringBuilder();
        ddl.append(createTablePrefix())
                .append(effectiveTableName(scenario.tableName()));
        ddl.append(" (");
        List<ColumnSpec> cols = scenario.columns();
        for (int i = 0; i < cols.size(); i++) {
            if (i > 0) {
                ddl.append(", ");
            }
            ddl.append(cols.get(i).name()).append(' ')
                    .append(toDdl(cols.get(i).type()));
        }
        if (scenario.blobColumn() != null) {
            String blobType = blobDdlType();
            if (blobType != null) {
                ddl.append(", ").append(scenario.blobColumn())
                        .append(' ').append(blobType);
            }
        }
        ddl.append(')');
        appendEngine(ddl, scenario);
        PartitionStyle effStyle = effectivePartitionStyle(scenario);
        if (effStyle != null) {
            String effColumn = effectivePartitionColumn(scenario, effStyle);
            appendPartition(ddl, effStyle, effColumn, scenario);
        }
        try (Statement st = conn.createStatement()) {
            st.execute(ddl.toString());
        }
    }

    protected PartitionStyle effectivePartitionStyle(TableScenario scenario) {
        PartitionStyle s = scenario.partitionStyle();
        if (s == null || supportedPartitions().contains(s)) {
            return s;
        }
        for (PartitionStyle fb : fallbackOrder()) {
            if (supportedPartitions().contains(fb)) {
                return fb;
            }
        }
        return null;
    }

    protected String effectivePartitionColumn(TableScenario scenario,
                                              PartitionStyle effStyle) {
        return effStyle == scenario.partitionStyle()
                ? scenario.partitionColumn()
                : scenario.keyColumn();
    }

    protected int partitionCount(TableScenario scenario) {
        long rows = scenario.oracle().rowCount();
        return Math.max(4, (int) (rows / 250_000L));
    }

    protected long[] rangeIntBoundaries(TableScenario scenario) {
        long n = scenario.oracle().rowCount();
        int p = partitionCount(scenario);
        long[] b = new long[p - 1];
        long chunk = n / p;
        for (int i = 0; i < p - 1; i++) {
            b[i] = (i + 1) * chunk;
        }
        return b;
    }

    protected abstract String toDdl(LogicalType type);

    protected String blobDdlType() {
        return null;
    }

    protected void appendEngine(StringBuilder ddl, TableScenario scenario) {
    }

    protected void appendPartition(StringBuilder ddl, PartitionStyle style,
                                   String column, TableScenario scenario) {
    }

    protected String createTablePrefix() {
        return "CREATE TABLE IF NOT EXISTS ";
    }
}
