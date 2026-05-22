package tech.ydb.importer.source;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
 * Source table metadata retrieval - ClickHouse specifics.
 */
public class ClickHouseTableLister extends AnyTableLister {

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseTableLister.class);

    public static final Set<String> SKIP_DATABASES;
    public static final Set<String> SKIP_TABLE_ENGINES;

    static {
        final Set<String> db = new HashSet<>();
        db.add("system");
        db.add("INFORMATION_SCHEMA");
        db.add("information_schema");
        SKIP_DATABASES = Collections.unmodifiableSet(db);

        final Set<String> eng = new HashSet<>();
        eng.add("View");
        SKIP_TABLE_ENGINES = Collections.unmodifiableSet(eng);
    }

    public ClickHouseTableLister(TableMapList tableMaps) {
        super(tableMaps);
    }

    @Override
    protected List<String> listSchemas(Connection con) throws SQLException {
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT name FROM system.databases")) {
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
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT name, engine FROM system.tables "
                + "WHERE database = ? AND is_temporary = 0 "
                + "ORDER BY name")) {
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
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT total_rows FROM system.tables "
                + "WHERE database = ? AND name = ? AND is_temporary = 0")) {
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
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT name FROM system.columns "
                + "WHERE database = ? AND table = ? "
                + "ORDER BY position")) {
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
    public List<TaskInfo> listPartitions(Connection con, TableDecision td, TableMetadata tm)
            throws SQLException {
        if (td.getTableRef() != null && td.getTableRef().hasQueryText()) {
            return Collections.emptyList();
        }
        String schema = td.getSchema();
        String table = td.getTable();
        String baseSql = makeSelectSql(schema, table, tm.getColumns());
        int chunkRows = tableMaps.getConfig().getSource().getFetchSize();
        final List<TaskInfo> tasks = new ArrayList<>();

        try (PreparedStatement ps = con.prepareStatement(
                "SELECT name, rows FROM system.parts "
                + "WHERE database = ? AND table = ? AND active "
                + "ORDER BY name")) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String partName = rs.getString(1);
                    long rows = rs.getLong(2);
                    if (rows <= 0) {
                        continue;
                    }
                    List<TaskQuery> queries = new ArrayList<>();
                    for (long offset = 0; offset < rows; offset += chunkRows) {
                        long end = Math.min(offset + chunkRows, rows);
                        String sql = String.format(
                                "%s WHERE _part = '%s' AND _part_offset >= %d AND _part_offset < %d",
                                baseSql, partName.replace("'", "''"), offset, end);
                        String name = schema + "." + table
                                + "#" + partName + "[" + offset + ":" + end + "]";
                        queries.add(new TaskQuery(name, sql));
                    }
                    tasks.add(new TaskInfo(schema + "." + table + "#" + partName, queries));
                }
            }
        }
        if (tasks.size() < 2) {
            return Collections.emptyList();
        }
        return tasks;
    }

    @Override
    public void beforeTableRead(Connection con, TableDecision td) throws SQLException {
        String sql = "SYSTEM STOP MERGES " + safeId(td.getSchema()) + "." + safeId(td.getTable());
        try (Statement stmt = con.createStatement()) {
            stmt.execute(sql);
        }
        LOG.info("Stopped merges for {}.{}", td.getSchema(), td.getTable());
        refreshPartitionTasks(con, td);
    }

    private void refreshPartitionTasks(Connection con, TableDecision td) throws SQLException {
        if (td.getTableRef() != null
                && (td.getTableRef().hasSplit() || td.getTableRef().hasQueryText())) {
            return;
        }
        if (!td.useSourcePartitions()) {
            return;
        }
        List<TaskInfo> fresh = listPartitions(con, td, td.getMetadata());
        if (fresh.isEmpty()) {
            String label = td.getSchema() + "." + td.getTable();
            fresh = Collections.singletonList(new TaskInfo(label,
                    makeSelectSql(td.getSchema(), td.getTable(), td.getMetadata().getColumns())));
        }
        td.getMetadata().setTasks(fresh);
    }

    @Override
    public void afterTableRead(Connection con, TableDecision td) throws SQLException {
        String sql = "SYSTEM START MERGES " + safeId(td.getSchema()) + "." + safeId(td.getTable());
        try (Statement stmt = con.createStatement()) {
            stmt.execute(sql);
        }
        LOG.info("Started merges for {}.{}", td.getSchema(), td.getTable());
    }

    @Override
    public boolean useStringForClobRead() {
        return true;
    }

    @Override
    protected String safeId(String id) {
        if (id.contains("`")) {
            throw new IllegalArgumentException("Backtick within the identifier: " + id);
        }
        return "`" + id + "`";
    }
}
