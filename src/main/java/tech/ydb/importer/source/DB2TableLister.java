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

/** Source table metadata retrieval - IBM Db2 LUW specifics. */
public class DB2TableLister extends AnyTableLister {

    private static final Logger LOG = LoggerFactory.getLogger(DB2TableLister.class);

    public static final Set<String> SKIP_SCHEMAS;

    static {
        final Set<String> x = new HashSet<>();
        x.add("NULLID");
        x.add("SQLJ");
        x.add("SYSPROC");
        x.add("SYSIBMADM");
        SKIP_SCHEMAS = Collections.unmodifiableSet(x);
    }

    public DB2TableLister(TableMapList tableMaps) {
        super(tableMaps);
    }

    @Override
    protected List<String> listSchemas(Connection con) throws SQLException {
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT SCHEMANAME FROM SYSCAT.SCHEMATA")) {
            try (ResultSet rs = ps.executeQuery()) {
                final List<String> retval = new ArrayList<>();
                while (rs.next()) {
                    String value = rs.getString(1);
                    if (value == null) {
                        continue;
                    }
                    value = value.trim();
                    if (value.startsWith("SYS") || value.startsWith("IBM")) {
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
                "SELECT TABNAME FROM SYSCAT.TABLES "
                + "WHERE TABSCHEMA=? AND TYPE='T' "
                + "ORDER BY TABNAME")) {
            ps.setString(1, schema);
            try (ResultSet rs = ps.executeQuery()) {
                final List<String> retval = new ArrayList<>();
                while (rs.next()) {
                    retval.add(rs.getString(1).trim());
                }
                return retval;
            }
        }
    }

    @Override
    protected long grabRowCount(Connection con, TableIdentity ti) throws SQLException {
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT CARD FROM SYSCAT.TABLES "
                + "WHERE TABSCHEMA=? AND TABNAME=?")) {
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
                "SELECT COLNAME FROM SYSCAT.COLUMNS "
                + "WHERE TABSCHEMA=? AND TABNAME=? "
                + "ORDER BY COLNO")) {
            ps.setString(1, ti.getSchema());
            ps.setString(2, ti.getTable());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    cols.add(new ColumnInfo(rs.getString(1).trim()));
                }
            }
        }
        return cols;
    }

    @Override
    protected void grabPrimaryKey(Connection con, TableIdentity ti, TableMetadata tm)
            throws SQLException {
        if (grabPrimaryKeyConstraint(con, ti, tm)) {
            return;
        }
        chooseBestUniqueIndexAsKey(con, ti, tm);
    }

    /**
     * Retrieve the real PRIMARY KEY definition, if it exists. Db2 PK columns
     * are implicitly NOT NULL.
     */
    private boolean grabPrimaryKeyConstraint(Connection con, TableIdentity ti, TableMetadata tm)
            throws SQLException {
        tm.clearKey();
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT k.COLNAME FROM SYSCAT.TABCONST c "
                + "JOIN SYSCAT.KEYCOLUSE k "
                + "  ON c.CONSTNAME=k.CONSTNAME "
                + " AND c.TABSCHEMA=k.TABSCHEMA "
                + " AND c.TABNAME=k.TABNAME "
                + "WHERE c.TABSCHEMA=? AND c.TABNAME=? AND c.TYPE='P' "
                + "ORDER BY k.COLSEQ")) {
            ps.setString(1, ti.getSchema());
            ps.setString(2, ti.getTable());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tm.addKey(rs.getString(1).trim());
                }
            }
        }
        return !tm.getKey().isEmpty();
    }

    /**
     * Choose the best UNIQUE index when there is no PRIMARY KEY. Matches
     * Postgres parity: both constraint-backed uniques (created via
     * {@code UNIQUE} constraint) and bare uniques (created via
     * {@code CREATE UNIQUE INDEX}) live in {@code SYSCAT.INDEXES} with
     * {@code UNIQUERULE='U'} - a single query covers both. The PK's
     * enforcing index has {@code UNIQUERULE='P'} and is excluded.
     * <p>
     * Ranking: smallest column count first, ties broken by index name.
     * Expression-based indexes (synthetic column refs absent from
     * {@code SYSCAT.COLUMNS}) are skipped.
     */
    private void chooseBestUniqueIndexAsKey(Connection con, TableIdentity ti, TableMetadata tm)
            throws SQLException {
        String bestIndSchema = null;
        String bestIndName = null;
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT i.INDSCHEMA, i.INDNAME FROM SYSCAT.INDEXES i "
                + "WHERE i.TABSCHEMA=? AND i.TABNAME=? "
                + "  AND i.UNIQUERULE='U' "
                + "  AND NOT EXISTS ("
                + "      SELECT 1 FROM SYSCAT.INDEXCOLUSE ic "
                + "      WHERE ic.INDSCHEMA=i.INDSCHEMA AND ic.INDNAME=i.INDNAME "
                + "        AND ic.COLORDER IN ('A','D') "
                + "        AND NOT EXISTS ("
                + "            SELECT 1 FROM SYSCAT.COLUMNS c "
                + "            WHERE c.TABSCHEMA=i.TABSCHEMA AND c.TABNAME=i.TABNAME "
                + "              AND c.COLNAME=ic.COLNAME)) "
                + "ORDER BY i.UNIQUE_COLCOUNT, i.INDNAME "
                + "FETCH FIRST 1 ROWS ONLY")) {
            ps.setString(1, ti.getSchema());
            ps.setString(2, ti.getTable());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    bestIndSchema = rs.getString(1).trim();
                    bestIndName = rs.getString(2).trim();
                }
            }
        }
        if (bestIndName == null) {
            return;
        }
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT COLNAME FROM SYSCAT.INDEXCOLUSE "
                + "WHERE INDSCHEMA=? AND INDNAME=? "
                + "  AND COLORDER IN ('A','D') "
                + "ORDER BY COLSEQ")) {
            ps.setString(1, bestIndSchema);
            ps.setString(2, bestIndName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tm.addKey(rs.getString(1).trim());
                }
            }
        }
    }

    @Override
    public List<PartitionInfo> listPartitions(Connection con, TableDecision td, TableMetadata tm)
            throws SQLException {
        if (td.getTableRef() != null && td.getTableRef().hasQueryText()) {
            return Collections.emptyList();
        }
        if (tm.getColumns().isEmpty()) {
            return Collections.emptyList();
        }
        final List<PartitionInfo> partitions = new ArrayList<>();
        final String baseSql = makeSelectSql(td.getSchema(), td.getTable(), tm.getColumns());
        // DATAPARTITIONNUM(col) returns the sequence number of the partition
        // that holds the row. Any column works - use the first one.
        final String partCol = safeId(tm.getColumns().get(0).getName());
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT SEQNO FROM SYSCAT.DATAPARTITIONS "
                + "WHERE TABSCHEMA=? AND TABNAME=? "
                + "ORDER BY SEQNO")) {
            ps.setString(1, td.getSchema());
            ps.setString(2, td.getTable());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    int seqno = rs.getInt(1);
                    String sql = baseSql
                            + " WHERE DATAPARTITIONNUM(" + partCol + ") = " + seqno;
                    String label = td.getSchema() + "." + td.getTable() + "#" + seqno;
                    partitions.add(new PartitionInfo(label, sql));
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
