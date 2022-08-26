package ydb.importer.source;

import java.util.Objects;
import ydb.importer.config.TableIdentity;

/**
 *
 * @author zinal
 */
public class SourceTableName implements TableIdentity {
    
    private final String schema;
    private final String table;

    public SourceTableName(String schema, String table) {
        this.schema = schema;
        this.table = table;
    }
    
    public SourceTableName(TableIdentity ti) {
        this.schema = ti.getSchema();
        this.table = ti.getTable();
    }
    
    @Override
    public String getSchema() {
        return schema;
    }

    @Override
    public String getTable() {
        return table;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 41 * hash + Objects.hashCode(this.schema);
        hash = 41 * hash + Objects.hashCode(this.table);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final SourceTableName other = (SourceTableName) obj;
        if (!Objects.equals(this.schema, other.schema)) {
            return false;
        }
        return Objects.equals(this.table, other.table);
    }
    
}
