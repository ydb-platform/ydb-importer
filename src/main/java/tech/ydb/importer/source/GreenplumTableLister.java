package tech.ydb.importer.source;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import tech.ydb.importer.TableDecision;

/**
 * Source table metadata retrieval - Greenplum specifics.
 */
public class GreenplumTableLister extends PostgresTableLister {

    private static final Set<String> EXTRA_SKIP_SCHEMAS =
            Collections.singleton("gp_toolkit");

    public GreenplumTableLister(TableMapList tableMaps) {
        super(tableMaps);
    }

    @Override
    protected List<String> listSchemas(Connection con) throws SQLException {
        List<String> schemas = super.listSchemas(con);
        schemas.removeIf(EXTRA_SKIP_SCHEMAS::contains);
        return schemas;
    }

    /**
     * Greenplum reports NOT NULL columns as nullable in ResultSetMetaData.
     * Restore the flag from pg_attribute.
     */
    @Override
    protected void grabColumnTypes(Connection con, TableDecision td, TableMetadata tm)
            throws SQLException {
        super.grabColumnTypes(con, td, tm);
        String sql = "SELECT a.attname "
                + "FROM pg_class c "
                + "  JOIN pg_namespace s ON s.oid = c.relnamespace "
                + "  JOIN pg_attribute a ON a.attrelid = c.oid "
                + "WHERE s.nspname = ? AND c.relname = ? "
                + "  AND a.attnum > 0 AND NOT a.attisdropped "
                + "  AND a.attnotnull";
        try (PreparedStatement ps = con.prepareStatement(sql)) {
            ps.setString(1, td.getSchema());
            ps.setString(2, td.getTable());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    ColumnInfo ci = tm.findColumn(rs.getString(1));
                    if (ci != null) {
                        ci.setNullable(false);
                    }
                }
            }
        }
    }
}
