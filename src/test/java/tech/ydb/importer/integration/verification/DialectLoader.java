package tech.ydb.importer.integration.verification;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.LongConsumer;

import tech.ydb.importer.integration.verification.TableScenario.PartitionStyle;

/** Per-dialect adapter: creates a table and loads rows for a {@link TableScenario} */
public abstract class DialectLoader {

    /** Creates a table for the scenario in the source DB */
    public void createTable(Connection conn, TableScenario scenario)
            throws SQLException {
        StringBuilder ddl = new StringBuilder();
        ddl.append(createTablePrefix())
                .append(adaptTableName(scenario.tableName()));
        ddl.append(" (");
        List<ColumnSpec> cols = scenario.columns();
        for (int i = 0; i < cols.size(); i++) {
            if (i > 0) {
                ddl.append(", ");
            }
            ddl.append(cols.get(i).name()).append(' ')
                    .append(columnDdl(cols.get(i)));
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
        PartitionStyle style = resolvePartitionStyle(scenario);
        if (style != null) {
            String column = resolvePartitionColumn(scenario, style);
            appendPartition(ddl, style, column, scenario);
        }
        try (Statement st = conn.createStatement()) {
            st.execute(ddl.toString());
        }
    }

    /** Inserts all rows from the scenario oracle using {@link LoaderUtil} */
    public void loadRows(Connection conn, TableScenario scenario,
                         int batchSize, LongConsumer onBatchFlush)
            throws SQLException {
        if (supportsMultiRowInsert()) {
            LoaderUtil.loadRows(conn, scenario, batchSize, onBatchFlush);
        } else {
            LoaderUtil.loadRowsBatched(conn, scenario, batchSize, onBatchFlush);
        }
    }

    /** Adjusts the scenario table name for the dialect */
    public String adaptTableName(String name) {
        return name;
    }

    public int rowsPerInsertStatement() {
        return 50_000;
    }

    public int loaderPoolSize() {
        return -1;
    }

    public boolean supportsMultiRowInsert() {
        return true;
    }

    public Object adjustExpected(LogicalType type, Object expected) {
        return expected;
    }

    /** Per-dialect hook invoked right after a new Connection is opened */
    public void onConnectionOpened(Connection conn, String schema)
            throws SQLException {
    }

    /** Partition styles the dialect can express */
    public Set<PartitionStyle> supportedPartitions() {
        return EnumSet.allOf(PartitionStyle.class);
    }

    /** Fallback partition styles tried in order when the scenario style is unsupported */
    public PartitionStyle[] fallbackOrder() {
        return new PartitionStyle[] {
                PartitionStyle.HASH_INT,
                PartitionStyle.RANGE_INT
        };
    }

    protected PartitionStyle resolvePartitionStyle(TableScenario scenario) {
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

    protected String resolvePartitionColumn(TableScenario scenario,
                                            PartitionStyle style) {
        return style == scenario.partitionStyle()
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

    protected static final DateTimeFormatter DATE_LITERAL_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    protected LocalDateTime[] rangeDateBoundaries(TableScenario scenario) {
        int p = partitionCount(scenario);
        LocalDateTime[] b = new LocalDateTime[p - 1];
        long chunkSeconds = Math.max(1L, ShopScenarios.TARGET_SECONDS / p);
        for (int i = 0; i < p - 1; i++) {
            b[i] = ShopScenarios.BASE_DT.plusSeconds((i + 1) * chunkSeconds);
        }
        return b;
    }

    protected abstract String toDdl(LogicalType type);

    protected String columnDdl(ColumnSpec col) {
        return toDdl(col.type());
    }

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
