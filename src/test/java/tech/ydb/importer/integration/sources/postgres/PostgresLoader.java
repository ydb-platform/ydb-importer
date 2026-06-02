package tech.ydb.importer.integration.sources.postgres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.function.LongConsumer;

import org.postgresql.PGConnection;
import org.postgresql.largeobject.LargeObject;
import org.postgresql.largeobject.LargeObjectManager;

import tech.ydb.importer.integration.verification.DialectLoader;
import tech.ydb.importer.integration.verification.LoaderUtil;
import tech.ydb.importer.integration.verification.LogicalType;
import tech.ydb.importer.integration.verification.TableScenario;
import tech.ydb.importer.integration.verification.TableScenario.PartitionStyle;

public class PostgresLoader extends DialectLoader {

    public static final PostgresLoader INSTANCE = new PostgresLoader();

    protected PostgresLoader() {
    }

    @Override
    protected String toDdl(LogicalType type) {
        switch (type) {
            case INT32:           return "integer";
            case INT64:           return "bigint";
            case DECIMAL_18_4:    return "numeric(18, 4)";
            case STRING:          return "varchar(255)";
            case BOOL:            return "boolean";
            case DATE:            return "date";
            case DATETIME:        return "timestamp";
            default:
                throw new IllegalArgumentException("Unsupported: " + type);
        }
    }

    @Override
    protected String blobDdlType() {
        return "oid";
    }

    @Override
    protected void appendPartition(StringBuilder ddl, PartitionStyle style,
                                   String column, TableScenario scenario) {
        switch (style) {
            case HASH_INT:
                ddl.append(" PARTITION BY HASH(").append(column).append(')');
                break;
            case RANGE_INT:
            case RANGE_DATE:
                ddl.append(" PARTITION BY RANGE(").append(column).append(')');
                break;
            case LIST_STRING:
                ddl.append(" PARTITION BY LIST(").append(column).append(')');
                break;
            default:
                break;
        }
    }

    @Override
    public void createTable(Connection conn, TableScenario scenario)
            throws SQLException {
        super.createTable(conn, scenario);
        if (scenario.partitionStyle() != null) {
            createPartitions(conn, scenario);
        }
    }

    private void createPartitions(Connection conn, TableScenario scenario)
            throws SQLException {
        String table = scenario.tableName();
        int p = partitionCount(scenario);
        try (Statement st = conn.createStatement()) {
            switch (scenario.partitionStyle()) {
                case HASH_INT:
                    for (int i = 0; i < p; i++) {
                        st.execute("CREATE TABLE " + table + "_p" + i
                                + " PARTITION OF " + table
                                + " FOR VALUES WITH (MODULUS " + p
                                + ", REMAINDER " + i + ")");
                    }
                    break;
                case RANGE_INT: {
                    long[] b = rangeIntBoundaries(scenario);
                    String from = "MINVALUE";
                    for (int i = 0; i < b.length; i++) {
                        st.execute("CREATE TABLE " + table + "_p" + i
                                + " PARTITION OF " + table
                                + " FOR VALUES FROM (" + from
                                + ") TO (" + b[i] + ")");
                        from = String.valueOf(b[i]);
                    }
                    st.execute("CREATE TABLE " + table + "_pmax PARTITION OF "
                            + table + " FOR VALUES FROM (" + from
                            + ") TO (MAXVALUE)");
                    break;
                }
                case RANGE_DATE: {
                    LocalDateTime[] db = rangeDateBoundaries(scenario);
                    String from = "MINVALUE";
                    for (int i = 0; i < db.length; i++) {
                        String to = "'" + db[i].format(DATE_LITERAL_FMT) + "'";
                        st.execute("CREATE TABLE " + table + "_p" + i
                                + " PARTITION OF " + table
                                + " FOR VALUES FROM (" + from + ") TO (" + to + ")");
                        from = to;
                    }
                    st.execute("CREATE TABLE " + table + "_pmax PARTITION OF "
                            + table + " FOR VALUES FROM (" + from + ") TO (MAXVALUE)");
                    break;
                }
                case LIST_STRING:
                    st.execute("CREATE TABLE " + table + "_card PARTITION OF "
                            + table + " FOR VALUES IN ('card')");
                    st.execute("CREATE TABLE " + table + "_cash PARTITION OF "
                            + table + " FOR VALUES IN ('cash')");
                    st.execute("CREATE TABLE " + table + "_transfer PARTITION OF "
                            + table + " FOR VALUES IN ('transfer')");
                    st.execute("CREATE TABLE " + table + "_crypto PARTITION OF "
                            + table + " FOR VALUES IN ('crypto')");
                    break;
                default:
                    break;
            }
        }
    }

    @Override
    public void loadRows(Connection conn, TableScenario scenario,
                         int batchSize, LongConsumer onBatchFlush) throws SQLException {
        if (scenario.blobColumn() == null) {
            LoaderUtil.loadRows(conn, scenario, batchSize, onBatchFlush);
            return;
        }
        boolean wasAutoCommit = conn.getAutoCommit();
        conn.setAutoCommit(false);
        try {
            LoaderUtil.loadRows(conn, scenario, batchSize,
                    PostgresLoader::writeLargeObject, onBatchFlush);
            conn.commit();
        } finally {
            conn.setAutoCommit(wasAutoCommit);
        }
    }

    private static void writeLargeObject(Connection conn, PreparedStatement ps,
                                         int index, byte[] blob)
            throws SQLException {
        if (blob == null) {
            ps.setNull(index, java.sql.Types.BIGINT);
            return;
        }
        LargeObjectManager lom = conn.unwrap(PGConnection.class)
                .getLargeObjectAPI();
        long oid = lom.createLO(LargeObjectManager.READWRITE);
        LargeObject lo = lom.open(oid, LargeObjectManager.WRITE);
        try {
            lo.write(blob);
        } finally {
            lo.close();
        }
        ps.setLong(index, oid);
    }
}
