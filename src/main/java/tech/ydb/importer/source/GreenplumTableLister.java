package tech.ydb.importer.source;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import tech.ydb.importer.TableDecision;

/**
 * Source table metadata retrieval - Greenplum specifics.
 * Greenplum 7 is wire-compatible with PostgreSQL and uses the same
 * system catalogs, so the PostgreSQL lister works without changes
 * except for nullability: Greenplum loses NOT NULL through the
 * metadata-probe subquery, so we restore it from pg_attribute.
 */
public class GreenplumTableLister extends PostgresTableLister {

    public GreenplumTableLister(TableMapList tableMaps) {
        super(tableMaps);
    }

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
