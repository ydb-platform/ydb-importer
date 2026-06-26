package tech.ydb.importer.source;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.importer.TableDecision;
import tech.ydb.importer.config.TableIdentity;

/**
 * Source table metadata retrieval - HANA specifics.
 */
public class HanaTableLister extends AnyTableLister {

    private static final Logger LOG = LoggerFactory.getLogger(HanaTableLister.class);

    public static final Set<String> SKIP_SCHEMAS;

    static {
        final Set<String> x = new HashSet<>();
        x.add("SYS");
        x.add("PUBLIC");
        x.add("HANA_XS_BASE");
        x.add("SYSTEM");
        SKIP_SCHEMAS = Collections.unmodifiableSet(x);
    }

    public HanaTableLister(TableMapList tableMaps) {
        super(tableMaps);
    }

    @Override
    protected List<String> listSchemas(Connection con) throws SQLException {
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT SCHEMA_NAME FROM SYS.SCHEMAS")) {
            try (ResultSet rs = ps.executeQuery()) {
                final List<String> retval = new ArrayList<>();
                while (rs.next()) {
                    String value = rs.getString(1);
                    if (value == null || value.startsWith("_SYS")) {
                        continue;
                    }
                    if (SKIP_SCHEMAS.contains(value)) {
                        continue;
                    }
                    retval.add(value);
                }
                return retval;
            }
        }
    }

    @Override
    protected List<String> listTables(Connection con, String schema) throws SQLException {
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT TABLE_NAME FROM SYS.TABLES "
                + "WHERE SCHEMA_NAME=? "
                + "ORDER BY TABLE_NAME")) {
            ps.setString(1, schema);
            try (ResultSet rs = ps.executeQuery()) {
                final List<String> retval = new ArrayList<>();
                while (rs.next()) {
                    retval.add(rs.getString(1));
                }
                return retval;
            }
        }
    }

    @Override
    protected long grabRowCount(Connection con, TableIdentity ti) throws SQLException {
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT RECORD_COUNT FROM SYS.M_TABLES "
                + "WHERE SCHEMA_NAME=? AND TABLE_NAME=?")) {
            ps.setString(1, ti.getSchema());
            ps.setString(2, ti.getTable());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    long retval = rs.getLong(1);
                    if (!rs.wasNull()) {
                        return retval;
                    }
                }
            }
        }
        return -1L;
    }

    @Override
    protected List<ColumnInfo> grabColumnNames(Connection con, TableIdentity ti) throws SQLException {
        final List<ColumnInfo> cols = new ArrayList<>();
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT COLUMN_NAME FROM SYS.TABLE_COLUMNS "
                + "WHERE SCHEMA_NAME=? AND TABLE_NAME=? "
                + "ORDER BY POSITION")) {
            ps.setString(1, ti.getSchema());
            ps.setString(2, ti.getTable());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    cols.add(new ColumnInfo(rs.getString(1)));
                }
            }
        }
        return cols;
    }

    /**
     * HANA JDBC reports TIMESTAMP with scale=0, which maps to Datetime64
     * and loses fractional seconds. Tag scale=7 so the mapper picks Timestamp64.
     * SMALLDECIMAL is reported as DECIMAL scale=0, which maps to Int64 and loses
     * the fraction. Tag scale=-1 so the mapper picks Double.
     */
    @Override
    protected void grabColumnTypes(Connection con, TableDecision td, TableMetadata tm)
            throws SQLException {
        super.grabColumnTypes(con, td, tm);
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT COLUMN_NAME, DATA_TYPE_NAME FROM SYS.TABLE_COLUMNS "
                + "WHERE SCHEMA_NAME=? AND TABLE_NAME=?")) {
            ps.setString(1, td.getSchema());
            ps.setString(2, td.getTable());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String dataType = rs.getString(2);
                    ColumnInfo ci = tm.findColumn(rs.getString(1));
                    if (ci == null) {
                        continue;
                    }
                    if ("TIMESTAMP".equals(dataType)
                            && ci.getSqlType() == java.sql.Types.TIMESTAMP) {
                        ci.setSqlScale(7);
                    } else if ("SMALLDECIMAL".equals(dataType)
                            && (ci.getSqlType() == java.sql.Types.DECIMAL
                                || ci.getSqlType() == java.sql.Types.NUMERIC)) {
                        ci.setSqlScale(-1);
                    }
                }
            }
        }
    }

    @Override
    protected void grabPrimaryKey(Connection con, TableIdentity ti, TableMetadata tm)
            throws SQLException {
        if (grabPrimaryKeyConstraint(con, ti, tm)) {
            return;
        }
        chooseBestUniqueConstraintAsKey(con, ti, tm);
    }

    /**
     * Retrieve the real PRIMARY KEY definition, if it exists
     */
    private boolean grabPrimaryKeyConstraint(Connection con, TableIdentity ti, TableMetadata tm)
            throws SQLException {
        tm.clearKey();
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT COLUMN_NAME FROM SYS.CONSTRAINTS "
                + "WHERE SCHEMA_NAME=? AND TABLE_NAME=? "
                + "  AND IS_PRIMARY_KEY='TRUE' "
                + "ORDER BY POSITION")) {
            ps.setString(1, ti.getSchema());
            ps.setString(2, ti.getTable());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tm.addKey(rs.getString(1));
                }
            }
        }
        return !tm.getKey().isEmpty();
    }

    /**
     * Choose the best UNIQUE constraint when there is no PRIMARY KEY.
     * <p>
     * (a) Prefer a constraint with the smallest number of columns.
     * (b) For equal length constraints, order by constraint name alphabetically.
     * <p>
     * SYS.CONSTRAINTS only lists UNIQUE constraints from CREATE TABLE or
     * ALTER TABLE. Expression based and partial UNIQUE indexes live in
     * SYS.INDEXES and do not appear here.
     */
    private void chooseBestUniqueConstraintAsKey(Connection con, TableIdentity ti, TableMetadata tm)
            throws SQLException {
        String bestConstraint = null;
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT CONSTRAINT_NAME FROM SYS.CONSTRAINTS "
                + "WHERE SCHEMA_NAME=? AND TABLE_NAME=? "
                + "  AND IS_UNIQUE_KEY='TRUE' "
                + "  AND IS_PRIMARY_KEY='FALSE' "
                + "GROUP BY CONSTRAINT_NAME "
                + "ORDER BY COUNT(*), CONSTRAINT_NAME "
                + "LIMIT 1")) {
            ps.setString(1, ti.getSchema());
            ps.setString(2, ti.getTable());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    bestConstraint = rs.getString(1);
                }
            }
        }
        if (bestConstraint == null) {
            return;
        }
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT COLUMN_NAME FROM SYS.CONSTRAINTS "
                + "WHERE SCHEMA_NAME=? AND TABLE_NAME=? AND CONSTRAINT_NAME=? "
                + "ORDER BY POSITION")) {
            ps.setString(1, ti.getSchema());
            ps.setString(2, ti.getTable());
            ps.setString(3, bestConstraint);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tm.addKey(rs.getString(1));
                }
            }
        }
    }

    @Override
    public List<TaskInfo> listPartitions(Connection con, TableDecision td, TableMetadata tm)
            throws SQLException {
        if (td.getTableRef() != null && td.getTableRef().hasQueryText()) {
            return Collections.emptyList();
        }
        final List<TaskInfo> partitions = new ArrayList<>();
        final String baseSql = makeSelectSql(td.getSchema(), td.getTable(), tm.getColumns());
        // Row store tables cannot be partitioned. The query returns empty for them
        // and readMetadata falls back to a single task full scan.
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT PART_ID FROM SYS.TABLE_PARTITIONS "
                + "WHERE SCHEMA_NAME=? AND TABLE_NAME=? "
                + "  AND PART_ID > 0 "
                + "ORDER BY PART_ID")) {
            ps.setString(1, td.getSchema());
            ps.setString(2, td.getTable());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    int partId = rs.getInt(1);
                    String sql = baseSql + " PARTITION (" + partId + ")";
                    String label = td.getSchema() + "." + td.getTable() + "#" + partId;
                    partitions.add(new TaskInfo(label, sql));
                }
            }
        }
        if (partitions.size() < 2) {
            return Collections.emptyList();
        }
        LOG.info("Table {}.{}: found {} partitions",
                td.getSchema(), td.getTable(), partitions.size());
        return partitions;
    }

    @Override
    protected String safeId(String id) {
        if (id.contains("\"")) {
            throw new IllegalArgumentException("Double quotes within the identifier: " + id);
        }
        return "\"" + id + "\"";
    }
}
