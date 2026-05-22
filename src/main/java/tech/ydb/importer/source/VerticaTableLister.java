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
 * Source table metadata retrieval - Vertica specifics.
 */
public class VerticaTableLister extends AnyTableLister {

    private static final Logger LOG = LoggerFactory.getLogger(VerticaTableLister.class);

    public static final Set<String> SKIP_SCHEMAS;

    static {
        final Set<String> x = new HashSet<>();
        x.add("v_catalog");
        x.add("v_monitor");
        x.add("v_internal");
        x.add("v_txtindex");
        x.add("v_func");
        SKIP_SCHEMAS = Collections.unmodifiableSet(x);
    }

    public VerticaTableLister(TableMapList tableMaps) {
        super(tableMaps);
    }

    @Override
    protected List<String> listSchemas(Connection con) throws SQLException {
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT schema_name FROM v_catalog.schemata "
                + "WHERE NOT is_system_schema")) {
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
                "SELECT table_name FROM v_catalog.tables "
                + "WHERE table_schema=? "
                + "ORDER BY table_name")) {
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
                "SELECT ps.row_count "
                + "FROM v_monitor.projection_storage ps "
                + "INNER JOIN v_catalog.projections p "
                + "  ON ps.projection_id=p.projection_id "
                + "WHERE ps.anchor_table_schema=? AND ps.anchor_table_name=? "
                + "  AND p.is_super_projection "
                + "ORDER BY ps.row_count DESC LIMIT 1")) {
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
                "SELECT column_name FROM v_catalog.columns "
                + "WHERE table_schema=? AND table_name=? "
                + "ORDER BY ordinal_position")) {
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
        tm.clearKey();
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT column_name FROM v_catalog.primary_keys "
                + "WHERE table_schema=? AND table_name=? "
                + "  AND is_enabled "
                + "ORDER BY ordinal_position")) {
            ps.setString(1, ti.getSchema());
            ps.setString(2, ti.getTable());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tm.addKey(rs.getString(1));
                }
            }
        }
        if (tm.getKey().isEmpty()) {
            grabBestEnabledUniqueConstraint(con, ti, tm);
        }
    }

    private void grabBestEnabledUniqueConstraint(Connection con, TableIdentity ti, TableMetadata tm)
            throws SQLException {
        long bestConstraintId = -1;
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT cc.constraint_id, COUNT(*) AS col_count "
                + "FROM v_catalog.constraint_columns cc "
                + "WHERE cc.table_schema=? AND cc.table_name=? "
                + "  AND cc.constraint_type='u' "
                + "  AND cc.is_enabled "
                + "GROUP BY cc.constraint_id, cc.constraint_name "
                + "ORDER BY col_count, cc.constraint_name, cc.constraint_id "
                + "LIMIT 1")) {
            ps.setString(1, ti.getSchema());
            ps.setString(2, ti.getTable());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    bestConstraintId = rs.getLong(1);
                    if (rs.wasNull()) {
                        bestConstraintId = -1;
                    }
                }
            }
        }
        if (bestConstraintId >= 0) {
            try (PreparedStatement ps = con.prepareStatement(
                    "SELECT column_name FROM v_catalog.constraint_columns "
                    + "WHERE constraint_id=? "
                    + "ORDER BY column_name")) {
                ps.setLong(1, bestConstraintId);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        tm.addKey(rs.getString(1));
                    }
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
        String partitionExpr = getPartitionExpression(con, td);
        if (partitionExpr == null || partitionExpr.isEmpty()) {
            return Collections.emptyList();
        }
        final List<TaskInfo> partitions = new ArrayList<>();
        final String baseSql = makeSelectSql(td.getSchema(), td.getTable(), tm.getColumns());
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT pt.partition_key, SUM(pt.ros_row_count) AS row_count "
                + "FROM v_monitor.partitions pt "
                + "INNER JOIN v_catalog.projections p "
                + "  ON pt.projection_id = p.projection_id "
                + "INNER JOIN v_monitor.projection_storage ps "
                + "  ON p.projection_id = ps.projection_id "
                + "WHERE ps.anchor_table_schema = ? AND ps.anchor_table_name = ? "
                + "  AND p.is_super_projection "
                + "GROUP BY pt.partition_key "
                + "ORDER BY pt.partition_key")) {
            ps.setString(1, td.getSchema());
            ps.setString(2, td.getTable());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String partKey = rs.getString(1);
                    if (partKey == null) {
                        continue;
                    }
                    String whereSql = baseSql + " WHERE " + partitionExpr + " = " + partKey;
                    String name = td.getSchema() + "." + td.getTable() + "#" + partKey;
                    partitions.add(new TaskInfo(name, whereSql));
                }
            }
        }
        if (partitions.size() < 2) {
            return Collections.emptyList();
        }
        LOG.info("Table {}.{}: found {} partitions by expression: {}",
                td.getSchema(), td.getTable(), partitions.size(), partitionExpr);
        return partitions;
    }

    private String getPartitionExpression(Connection con, TableDecision td) throws SQLException {
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT partition_expression FROM v_catalog.tables "
                + "WHERE table_schema = ? AND table_name = ?")) {
            ps.setString(1, td.getSchema());
            ps.setString(2, td.getTable());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String expr = rs.getString(1);
                    if (rs.wasNull() || expr == null) {
                        return null;
                    }
                    return expr.trim();
                }
            }
        }
        return null;
    }

    @Override
    protected String safeId(String id) {
        if (id.contains("\"")) {
            throw new IllegalArgumentException("Double quotes within the identifier: " + id);
        }
        return "\"" + id + "\"";
    }
}
