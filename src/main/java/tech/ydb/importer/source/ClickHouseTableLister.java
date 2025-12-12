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
 * Source table metadata retrieval - ClickHouse specifics.
 */
public class ClickHouseTableLister extends AnyTableLister {

    public static final Set<String> SKIP_DATABASES;
    public static final Set<String> SKIP_TABLE_ENGINES;

    static {
        final Set<String> db = new HashSet<>();
        db.add("system");
        db.add("INFORMATION_SCHEMA");
        db.add("information_schema");
        SKIP_DATABASES = Collections.unmodifiableSet(db);

        final Set<String> eng = new HashSet<>();
        // TODO: what other engines to skip?
        eng.add("View");
        SKIP_TABLE_ENGINES = Collections.unmodifiableSet(eng);
    }

    public ClickHouseTableLister(TableMapList tableMaps) {
        super(tableMaps);
    }

    @Override
    protected List<String> listSchemas(Connection con) throws SQLException {
        final String sql = "SELECT name FROM system.databases";
        try (PreparedStatement ps = con.prepareStatement(sql)) {
            try (ResultSet rs = ps.executeQuery()) {
                final List<String> retval = new ArrayList<>();
                while (rs.next()) {
                    String value = rs.getString(1);
                    if (SKIP_DATABASES.contains(value)) {
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
        final String sql = ""
                + "SELECT name, engine "
                + "FROM system.tables "
                + "WHERE database = ? "
                + "  AND is_temporary = 0 "
                + "ORDER BY name";
        try (PreparedStatement ps = con.prepareStatement(sql)) {
            ps.setString(1, schema);
            try (ResultSet rs = ps.executeQuery()) {
                final List<String> retval = new ArrayList<>();
                while (rs.next()) {
                    String name = rs.getString(1);
                    String engine = rs.getString(2);
                    if (engine != null && SKIP_TABLE_ENGINES.contains(engine)) {
                        continue;
                    }
                    retval.add(name);
                }
                return retval;
            }
        }
    }

    @Override
    protected long grabRowCount(Connection con, TableIdentity ti) throws SQLException {
        final String sqlTables = ""
                + "SELECT total_rows "
                + "FROM system.tables "
                + "WHERE database = ? "
                + "  AND name = ? "
                + "  AND is_temporary = 0";
        try (PreparedStatement ps = con.prepareStatement(sqlTables)) {
            ps.setString(1, ti.getSchema());
            ps.setString(2, ti.getTable());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    long v = rs.getLong(1);
                    if (!rs.wasNull()) {
                        return v;
                    }
                }
            }
        }
        return -1L;
    }

    @Override
    protected List<ColumnInfo> grabColumnNames(Connection con, TableIdentity ti) throws SQLException {
        final String sql = ""
                + "SELECT name "
                + "FROM system.columns "
                + "WHERE database = ? "
                + "  AND table = ? "
                + "ORDER BY position";
        try (PreparedStatement ps = con.prepareStatement(sql)) {
            ps.setString(1, ti.getSchema());
            ps.setString(2, ti.getTable());
            try (ResultSet rs = ps.executeQuery()) {
                final List<ColumnInfo> cols = new ArrayList<>();
                while (rs.next()) {
                    cols.add(new ColumnInfo(rs.getString(1)));
                }
                return cols;
            }
        }
    }

    @Override
    protected void grabPrimaryKey(Connection con, TableIdentity ti, TableMetadata tm)
            throws SQLException {
        tm.clearKey();
    }

    @Override
    protected String safeId(String id) {
        if (id.contains("`")) {
            throw new IllegalArgumentException("Backtick within the identifier: " + id);
        }
        return "`" + id + "`";
    }
}
