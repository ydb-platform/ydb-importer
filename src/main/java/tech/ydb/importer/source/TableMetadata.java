package tech.ydb.importer.source;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Source table metadata, including:
 *  - the approximate number of rows (or -1 if not known)
 *  - list of column names in the order defined, plus their data types
 *  - list of columns included in the primary key
 * @author zinal
 */
public class TableMetadata {
    
    private long rowCount = -1;
    private final List<ColumnInfo> columns = new ArrayList<>();
    private final Map<String, ColumnInfo> lookup = new HashMap<>();
    private final List<ColumnInfo> key = new ArrayList<>();
    private String basicSql = null;
    
    public boolean isValid() {
        return !columns.isEmpty();
    }

    public long getRowCount() {
        return rowCount;
    }

    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    public void clearColumns() {
        columns.clear();
        lookup.clear();
        key.clear();
    }

    public List<ColumnInfo> getColumns() {
        return Collections.unmodifiableList(columns);
    }
    
    public void addColumn(ColumnInfo ci) {
        if (lookup.containsKey(ci.getName()))
            throw new IllegalArgumentException("Duplicate column: " + ci.getName());
        columns.add(ci);
        ci.setPosition(columns.size());
        lookup.put(ci.getName(), ci);
    }
    
    public void addColumns(List<ColumnInfo> cis) {
        for (ColumnInfo ci : cis)
            addColumn(ci);
    }

    public void addColumn(String name, int sqlType, int sqlPrecision, int sqlScale) {
        final ColumnInfo ci = new ColumnInfo(name);
        ci.setSqlType(sqlType);
        ci.setSqlPrecision(sqlPrecision);
        ci.setSqlScale(sqlScale);
        this.addColumn(ci);
    }
    
    public ColumnInfo findColumn(String name) {
        return lookup.get(name);
    }

    public ColumnInfo getColumn(String name) {
        ColumnInfo ci = findColumn(name);
        if (ci==null)
            throw new IllegalArgumentException("TableMetadata.getColumn(): " + name);
        return ci;
    }

    public List<ColumnInfo> getKey() {
        return Collections.unmodifiableList(key);
    }
    
    public void addKey(String name) {
        key.add(getColumn(name));
    }

    public void addKeys(List<String> names) {
        for (String name : names) {
            key.add(getColumn(name));
        }
    }

    public void clearKey() {
        key.clear();
    }

    public String getBasicSql() {
        return basicSql;
    }

    public void setBasicSql(String basicSql) {
        this.basicSql = basicSql;
    }

}
