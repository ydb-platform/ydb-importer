package tech.ydb.importer.source;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import tech.ydb.importer.config.TableIdentity;

/**
 * Source table metadata retrieval using generic JDBC APIs.
 * @author zinal
 */
public class GenericJdbcTableLister extends AnyTableLister {

    public GenericJdbcTableLister(TableMapList tableMaps) {
        super(tableMaps);
    }

    @Override
    protected List<String> listSchemas(Connection con) throws SQLException {
        try (ResultSet rs = con.getMetaData().getSchemas()) {
            int namePos = rs.findColumn("TABLE_SCHEM");
            final List<String> retval = new ArrayList<>();
            while (rs.next()) {
                retval.add(rs.getString(namePos));
            }
            return retval;
        }
    }

    @Override
    protected List<String> listTables(Connection con, String schema) throws SQLException {
        try (ResultSet rs = con.getMetaData().getTables(null, schema, null, new String[]{"TABLE"})) {
            int namePos = rs.findColumn("TABLE_NAME");
            final List<String> retval = new ArrayList<>();
            while (rs.next()) {
                retval.add(rs.getString(namePos));
            }
            return retval;
        }
    }

    @Override
    protected long grabRowCount(Connection con, TableIdentity ti) throws SQLException {
        try (ResultSet rs = con.getMetaData().getIndexInfo(null, ti.getSchema(), ti.getTable(), false, true)) {
            int posCard = rs.findColumn("CARDINALITY");
            int posType = rs.findColumn("TYPE");
            while (rs.next()) {
                short indexType = rs.getShort(posType);
                if (DatabaseMetaData.tableIndexStatistic == indexType) {
                    return rs.getLong(posCard);
                }
            }
        }
        return -1L;
    }

    @Override
    protected List<ColumnInfo> grabColumnNames(Connection con, TableIdentity ti) throws SQLException {
        // Retrieve the list of columns
        try (ResultSet rs = con.getMetaData().getColumns(null, ti.getSchema(), ti.getTable(), null)) {
            int namePos = rs.findColumn("COLUMN_NAME");
            final List<ColumnInfo> cols = new ArrayList<>();
            while (rs.next()) {
                cols.add( new ColumnInfo(rs.getString(namePos) ) );
            }
            return cols;
        }
    }

    @Override
    protected void grabPrimaryKey(Connection con, TableIdentity ti, TableMetadata tm) 
            throws SQLException {
        try (ResultSet rs = con.getMetaData().getPrimaryKeys(null, ti.getSchema(), ti.getTable())) {
            TreeMap<Integer,String> items = new TreeMap<>();
            int namePos = rs.findColumn("COLUMN_NAME");
            int seqPos = rs.findColumn("KEY_SEQ");
            while (rs.next()) {
                items.put(rs.getInt(seqPos), rs.getString(namePos));
            }
            for (String name : items.values()) {
                tm.addKey(name);
            }
        }
        if (tm.getKey().isEmpty()) {
            // If the key was not defined as a PK constraint, look for unique indexes.
            try (ResultSet rs = con.getMetaData().getIndexInfo(null, ti.getSchema(), ti.getTable(), true, false)) {
                String prevIndex = null;
                int posName = rs.findColumn("INDEX_NAME");
                int posColumn = rs.findColumn("COLUMN_NAME");
                int posType = rs.findColumn("TYPE");
                while (rs.next()) {
                    short indexType = rs.getShort(posType);
                    if (DatabaseMetaData.tableIndexStatistic == indexType) {
                        continue;
                    }
                    String indexName = rs.getString(posName);
                    if (prevIndex == null) {
                        prevIndex = indexName;
                    } else if (! prevIndex.equals(indexName)) {
                        break;
                    }
                    tm.addKey(rs.getString(posColumn));
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
