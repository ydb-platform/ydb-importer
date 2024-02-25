package tech.ydb.importer.source;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import tech.ydb.importer.config.TableIdentity;

/**
 * Source table metadata retrieval - Oracle specifics.
 * @author zinal
 */
public class OracleTableLister extends AnyTableLister {

    public OracleTableLister(TableMapList tableMaps) {
        super(tableMaps);
    }

    @Override
    protected List<String> listSchemas(Connection con) throws SQLException {
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT USERNAME FROM ALL_USERS WHERE ORACLE_MAINTAINED='N'")) {
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
    protected List<String> listTables(Connection con, String schema) throws SQLException {
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT table_name FROM all_tables WHERE owner=? AND status='VALID'")) {
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
                "SELECT num_rows FROM all_tables "
                + "WHERE owner=? AND table_name=?")) {
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
                "SELECT column_name FROM all_tab_columns "
                + "WHERE owner=? AND table_name=? ORDER BY column_id")) {
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
        try (PreparedStatement ps = con.prepareStatement("SELECT cols.column_name "
                + "FROM all_constraints cons, all_cons_columns cols "
                + "WHERE cols.owner = ? "
                + "  AND cols.table_name = ? "
                + "  AND cons.constraint_type = 'P' "
                + "  AND cons.constraint_name = cols.constraint_name "
                + "  AND cons.owner = cols.owner "
                + "ORDER BY cols.position")) {
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
            try (PreparedStatement ps = con.prepareStatement(""
                    + "SELECT ix.owner, ix.index_name, COUNT(*) AS col_count "
                    + "FROM all_indexes ix "
                    + "INNER JOIN all_ind_columns ic "
                    + "  ON ix.owner=ic.index_owner AND ix.index_name=ic.index_name "
                    + "WHERE ix.uniqueness='UNIQUE' "
                    + "  AND ix.table_owner=? AND ix.table_name=?"
                    + "GROUP BY ix.owner, ix.index_name "
                    + "ORDER BY COUNT(*), ix.index_name")) {
                ps.setString(1, ti.getSchema());
                ps.setString(2, ti.getTable());
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        String ixSchema = rs.getString(1);
                        String ixName = rs.getString(2);
                        grabIndexColumns(con, ixSchema, ixName, tm);
                    }
                }
            }
        }
    }

    private void grabIndexColumns(Connection con, String ixSchema, String ixName, TableMetadata tm) 
            throws SQLException {
        try (PreparedStatement ps = con.prepareStatement("SELECT column_name FROM all_ind_columns "
                + "WHERE index_owner=? AND index_name=? "
                + "ORDER BY column_position")) {
            ps.setString(1, ixSchema);
            ps.setString(2, ixName);
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

}
