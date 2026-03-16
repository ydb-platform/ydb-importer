package tech.ydb.importer.source;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.importer.TableDecision;
import tech.ydb.importer.config.TableIdentity;

/**
 * Source table metadata retrieval - MariaDB specifics.
 */
public class MariaDbTableLister extends MySqlTableLister {

    private static final Logger LOG = LoggerFactory.getLogger(MariaDbTableLister.class);

    public static final Set<String> SKIP_SCHEMAS;

    static {
        final Set<String> x = new HashSet<>();
        x.add("information_schema");
        x.add("mysql");
        x.add("performance_schema");
        SKIP_SCHEMAS = Collections.unmodifiableSet(x);
    }

    private final Map<String, String> tableEngines = new HashMap<>();

    public MariaDbTableLister(TableMapList tableMaps) {
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
                "SELECT table_name, engine FROM information_schema.tables "
                + "WHERE table_schema=? AND table_type='BASE TABLE'")) {
            ps.setString(1, schema);
            try (ResultSet rs = ps.executeQuery()) {
                final List<String> retval = new ArrayList<>();
                while (rs.next()) {
                    String tableName = rs.getString(1);
                    String engine = rs.getString(2);
                    retval.add(tableName);
                    if (engine != null) {
                        tableEngines.put(schema + "." + tableName, engine);
                    }
                }
                return retval;
            }
        }
    }

    @Override
    protected void grabPrimaryKey(Connection con, TableIdentity ti, TableMetadata tm)
            throws SQLException {
        String engine = tableEngines.get(ti.getSchema() + "." + ti.getTable());
        if (engine != null && engine.equalsIgnoreCase("Columnstore")) {
            tm.clearKey();
            return;
        }
        super.grabPrimaryKey(con, ti, tm);
    }

    @Override
    public List<PartitionInfo> listPartitions(Connection con, TableDecision td, TableMetadata tm)
            throws SQLException {
        if (td.getTableRef() != null && td.getTableRef().hasQueryText()) {
            return Collections.emptyList();
        }
        String engine = tableEngines.get(td.getSchema() + "." + td.getTable());
        if (engine != null && engine.equalsIgnoreCase("Columnstore")) {
            return Collections.emptyList();
        }
        final List<PartitionInfo> partitions = new ArrayList<>();
        final String baseSql = makeSelectSql(td.getSchema(), td.getTable(), tm.getColumns());
        try (PreparedStatement ps = con.prepareStatement(
                "SELECT partition_name FROM information_schema.partitions "
                + "WHERE table_schema=? AND table_name=? "
                + "  AND partition_name IS NOT NULL "
                + "ORDER BY partition_ordinal_position")) {
            ps.setString(1, td.getSchema());
            ps.setString(2, td.getTable());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String partName = rs.getString(1);
                    String sql = baseSql + " PARTITION (" + safeId(partName) + ")";
                    String label = td.getSchema() + "." + td.getTable() + "#" + partName;
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
}
