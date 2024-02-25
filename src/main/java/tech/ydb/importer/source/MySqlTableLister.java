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
import tech.ydb.importer.config.TableIdentity;

/**
 * Source table metadata retrieval - MySQL specifics.
 * @author zinal
 */
public class MySqlTableLister extends AnyTableLister {
    
    public static final Set<String> SKIP_SCHEMAS;
    static {
        final Set<String> x = new HashSet<>();
        x.add("information_schema");
        SKIP_SCHEMAS = Collections.unmodifiableSet(x);
    }

    public MySqlTableLister(TableMapList tableMaps) {
        super(tableMaps);
    }

    @Override
    protected List<String> listSchemas(Connection con) throws SQLException {
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT schema_name FROM information_schema.schemata")) {
            try (ResultSet rs = ps.executeQuery()) {
                final List<String> retval = new ArrayList<>();
                while (rs.next()) {
                    String value = rs.getString(1);
                    if (SKIP_SCHEMAS.contains(value))
                        continue;
                    retval.add(value);
                }
                return retval;
            }
        }
    }

    @Override
    protected List<String> listTables(Connection con, String schema) throws SQLException {
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT table_name FROM information_schema.tables "
                    + "WHERE table_schema=? AND table_type='BASE TABLE'")) {
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
                "SELECT table_rows FROM information_schema.tables "
                    + "WHERE table_schema=? AND table_name=?")) {
            ps.setString(1, ti.getSchema());
            ps.setString(2, ti.getTable());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    long retval = rs.getLong(1);
                    if (!rs.wasNull())
                        return retval;
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
                "SELECT column_name FROM information_schema.columns "
                    + "WHERE table_schema=? AND table_name=? "
                    + "ORDER BY ordinal_position")) {
            ps.setString(1, ti.getSchema());
            ps.setString(2, ti.getTable());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    cols.add( new ColumnInfo(rs.getString(1) ) );
                }
            }
        }
        return cols;
    }

    @Override
    protected void grabPrimaryKey(Connection con, TableIdentity ti, TableMetadata tm) 
            throws SQLException {
        // Retrieve the primary key (if one is defined)
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT kcu.column_name FROM information_schema.key_column_usage kcu "
                    + "INNER JOIN information_schema.table_constraints tc "
                    + "  ON kcu.constraint_name=tc.constraint_name "
                    + "  AND kcu.table_schema=tc.table_schema "
                    + "  AND kcu.table_name=tc.table_name "
                    + "WHERE tc.table_schema=? AND tc.table_name=? "
                    + "  AND tc.constraint_type='PRIMARY KEY' "
                    + "ORDER BY kcu.ordinal_position")) {
            ps.setString(1, ti.getSchema());
            ps.setString(2, ti.getTable());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tm.addKey(rs.getString(1));
                }
            }
        }
        if (tm.getKey().isEmpty()) {
            // If the key was not defined as a PK constraint, look for unique indexes.
            // MAYBE: logic to prefer all-integer keys over varchar-based.
            try (PreparedStatement ps = con.prepareStatement(
                    "SELECT tc.constraint_name, COUNT(*) "
                        + "FROM information_schema.table_constraints tc "
                        + "INNER JOIN information_schema.key_column_usage kcu "
                        + "  ON kcu.constraint_name=tc.constraint_name "
                        + "  AND kcu.table_schema=tc.table_schema "
                        + "  AND kcu.table_name=tc.table_name "
                        + "WHERE tc.table_schema=? AND tc.table_name=? "
                        + "  AND tc.constraint_type='UNIQUE' "
                        + "GROUP BY tc.constraint_name "
                        + "ORDER BY COUNT(*), tc.constraint_name")) {
                ps.setString(1, ti.getSchema());
                ps.setString(2, ti.getTable());
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        String consname = rs.getString(1);
                        if (! rs.wasNull() && consname!=null)
                            grabIndexColumns(con, consname, ti, tm);
                    }
                }
            }
        }
    }

    private void grabIndexColumns(Connection con, String consname,
            TableIdentity ti, TableMetadata tm) throws SQLException {
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT kcu.column_name FROM information_schema.key_column_usage kcu "
                    + "INNER JOIN information_schema.table_constraints tc "
                    + "  ON kcu.constraint_name=tc.constraint_name "
                    + "  AND kcu.table_schema=tc.table_schema "
                    + "  AND kcu.table_name=tc.table_name "
                    + "WHERE tc.table_schema=? AND tc.table_name=? "
                    + "  AND tc.constraint_name=? "
                    + "ORDER BY kcu.ordinal_position")) {
            ps.setString(1, ti.getSchema());
            ps.setString(2, ti.getTable());
            ps.setString(3, consname);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tm.addKey(rs.getString(1));
                }
            }
        }
    }

    @Override
    protected String safeId(String id) {
        if (id.contains("`")) {
            throw new IllegalArgumentException("Backticks within the identifier: " + id);
        }
        return "`" + id + "`";
    }
}
