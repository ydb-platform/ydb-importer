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

import tech.ydb.importer.TableDecision;
import tech.ydb.importer.config.TableIdentity;

/**
 * Source table metadata retrieval - PostgreSQL specifics.
 *
 * @author zinal
 */
public class PostgresTableLister extends AnyTableLister {

    public static final Set<String> SKIP_SCHEMAS;

    static {
        final Set<String> x = new HashSet<>();
        x.add("information_schema");
        x.add("pg_catalog");
        x.add("pg_toast");
        x.add("pg_temp_1");
        x.add("pg_toast_temp_1");
        SKIP_SCHEMAS = Collections.unmodifiableSet(x);
    }

    public PostgresTableLister(TableMapList tableMaps) {
        super(tableMaps);
    }

    @Override
    protected List<String> listSchemas(Connection con) throws SQLException {
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT nspname FROM pg_catalog.pg_namespace")) {
            try (ResultSet rs = ps.executeQuery()) {
                final List<String> retval = new ArrayList<>();
                while (rs.next()) {
                    String value = rs.getString(1);
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
                "SELECT c.relname  "
                + "FROM pg_catalog.pg_class c "
                + "INNER JOIN pg_catalog.pg_namespace n "
                + "  ON c.relnamespace = n.\"oid\" "
                + "WHERE c.relkind IN ('p', 'r') "
                + "  AND NOT c.relispartition "
                + "  AND n.nspname = ?")) {
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
        // Retrieve the approximate number of rows in the table
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT c.reltuples FROM pg_catalog.pg_class c "
                + "INNER JOIN pg_catalog.pg_namespace n "
                + "  ON n.\"oid\" = c.relnamespace "
                + "WHERE n.nspname = ? AND c.relname = ?")) {
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
        // Retrieve the list of columns
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT a.attname "
                + "FROM pg_catalog.pg_attribute a "
                + "INNER JOIN pg_catalog.pg_class c "
                + "  ON c.\"oid\" = a.attrelid "
                + "INNER JOIN pg_catalog.pg_namespace n "
                + "  ON n.\"oid\" = c.relnamespace "
                + "WHERE n.nspname = ? AND c.relname = ?"
                + "  AND a.attnum > 0 "
                + "ORDER BY a.attnum ")) {
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

    @Override
    protected void grabPrimaryKey(Connection con, TableIdentity ti, TableMetadata tm)
            throws SQLException {
        if (grabPrimaryKeyConstraint(con, ti, tm)) {
            return;
        }
        chooseBestUniqueIndexAsKey(con, ti, tm);
    }

    /**
     * Retrieve the real PRIMARY KEY definition, if it exists
     */
    private boolean grabPrimaryKeyConstraint(Connection con, TableIdentity ti, TableMetadata tm)
            throws SQLException {
        tm.clearKey();
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT ia.attname "
                        + "FROM pg_catalog.pg_attribute ia "
                        + "INNER JOIN pg_catalog.pg_class ic "
                        + "  ON ic.\"oid\" = ia.attrelid "
                        + "INNER JOIN pg_catalog.pg_index ix "
                        + "  ON ix.indexrelid = ic.\"oid\" "
                        + "INNER JOIN pg_catalog.pg_constraint x "
                        + "  ON x.conindid = ix.indexrelid "
                        + "INNER JOIN pg_catalog.pg_class c "
                        + "  ON c.\"oid\" = x.conrelid "
                        + "INNER JOIN pg_catalog.pg_namespace n "
                        + "  ON n.\"oid\" = c.relnamespace "
                        + "WHERE n.nspname = ? AND c.relname = ? "
                        + "  AND x.contype = 'p' "
                        + "  AND ix.indisvalid AND ix.indisunique "
                        + "ORDER BY ia.attnum")) {
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
     * Choose the best UNIQUE or UNIQUE index when there is no PRIMARY KEY.
     * <p>
     * (a) Prefer an index with the smallest number of columns.
     * (b) For equal-length indexes, compare sorted column-name lists.
     */
    private void chooseBestUniqueIndexAsKey(Connection con, TableIdentity ti, TableMetadata tm)
            throws SQLException {
        Long bestIndexId = null;
        int bestKeyCount = 0;

        try (PreparedStatement ps = con.prepareStatement(
                "WITH unique_indexes AS ("
                        + "  SELECT ix.indexrelid, "
                        + "         ix.indnkeyatts AS key_count, "
                        + "         array_agg(ia.attname ORDER BY ia.attname) AS sorted_columns "
                        + "  FROM pg_catalog.pg_index ix "
                        + "  INNER JOIN pg_catalog.pg_class c "
                        + "    ON c.\"oid\" = ix.indrelid "
                        + "  INNER JOIN pg_catalog.pg_namespace n "
                        + "    ON n.\"oid\" = c.relnamespace "
                        + "  INNER JOIN pg_catalog.pg_attribute ia "
                        + "    ON ia.attrelid = ix.indexrelid "
                        + "   AND ia.attnum > 0 "
                        + "   AND NOT ia.attisdropped "
                        + "   AND ia.attnum <= ix.indnkeyatts "
                        + "  LEFT JOIN pg_catalog.pg_constraint x "
                        + "    ON x.conindid = ix.indexrelid AND x.contype = 'p' "
                        + "  WHERE n.nspname = ? AND c.relname = ? "
                        + "    AND ix.indisvalid AND ix.indisunique "
                        + "    AND x.conindid IS NULL "
                        + "  GROUP BY ix.indexrelid, ix.indnkeyatts"
                        + ") "
                        + "SELECT indexrelid, key_count "
                        + "FROM unique_indexes "
                        + "ORDER BY key_count, sorted_columns, indexrelid "
                        + "LIMIT 1")) {
            ps.setString(1, ti.getSchema());
            ps.setString(2, ti.getTable());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    bestIndexId = rs.getLong(1);
                    if (rs.wasNull()) {
                        bestIndexId = null;
                    } else {
                        bestKeyCount = rs.getInt(2);
                    }
                }
            }
        }

        if (bestIndexId != null && bestKeyCount > 0) {
            readIndexColumns(con, bestIndexId, bestKeyCount, tm);
        }
    }

    private void readIndexColumns(Connection con, long indexRelId, int keyCount, TableMetadata tm)
            throws SQLException {
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT ia.attname "
                + "FROM pg_catalog.pg_attribute ia "
                + "WHERE ia.attrelid = ? "
                + "  AND ia.attnum > 0 "
                + "  AND NOT ia.attisdropped "
                + "  AND ia.attnum <= ? "
                + "ORDER BY ia.attnum")) {
            ps.setLong(1, indexRelId);
            ps.setInt(2, keyCount);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tm.addKey(rs.getString(1));
                }
            }
        }
    }

    @Override
    protected String safeId(String id) {
        if (id.contains("\"")) {
            throw new IllegalArgumentException("Double quotes within the identifier: " + id);
        }
        return "\"" + id + "\"";
    }

    @Override
    protected void grabColumnTypes(Connection con, TableDecision td, TableMetadata tm)
            throws SQLException {
        // Basic implementation comes from the parent.
        super.grabColumnTypes(con, td, tm);
        // Grab the BLOB columns, which are a magic in PostgreSQL.
        final String sqlBlob = ""
                + "SELECT a.attname "
                + "FROM pg_class C, pg_attribute a, pg_namespace s, pg_type t "
                + "WHERE a.attnum > 0 AND NOT a.attisdropped "
                + "  AND a.attrelid = c.oid "
                + "  AND a.atttypid = t.oid "
                + "  AND c.relnamespace = s.oid"
                + "  AND t.typname in ('oid', 'lo') "
                + "  AND c.relkind='r' "
                + "  AND s.nspname=? AND c.relname=?";
        try (PreparedStatement ps = con.prepareStatement(sqlBlob)) {
            ps.setString(1, td.getSchema());
            ps.setString(2, td.getTable());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String colname = rs.getString(1);
                    ColumnInfo ci = tm.findColumn(colname);
                    if (ci != null) {
                        ci.setSqlType(java.sql.Types.BLOB);
                        ci.setBlobAsObject(true);
                    }
                }
            }
        }
    }

}
