package tech.ydb.importer;

import tech.ydb.importer.target.TargetTable;
import java.util.HashMap;
import java.util.Map;
import tech.ydb.importer.config.TableOptions;
import tech.ydb.importer.config.TableRef;
import tech.ydb.importer.config.TableIdentity;
import tech.ydb.importer.source.TableMetadata;
import static tech.ydb.importer.config.JdomHelper.isBlank;

/**
 * Table lister decision to process the particular table.
 * @author zinal
 */
public class TableDecision implements TableIdentity {

    private final String schema;
    private final String table;
    private final TableOptions options;
    private final TableRef tableRef; // for table-ref defined only
    private TableMetadata metadata; // fetched from the source database
    private boolean failure;
    private TargetTable target;
    // blob column name -> blob table definition
    private final Map<String, TargetTable> blobTargets = new HashMap<>();

    public TableDecision(String schema, String table, TableOptions options) {
        this.schema = schema;
        this.table = table;
        this.options = options;
        this.tableRef = null;
        this.metadata = null;
        this.failure = false;
    }
    
    public TableDecision(TableRef tr) {
        this.schema = tr.getSchema();
        this.table = tr.getTable();
        this.options = tr.getOptions();
        this.tableRef = tr;
        this.metadata = null;
        this.failure = false;
    }

    @Override
    public String getSchema() {
        return schema;
    }

    @Override
    public String getTable() {
        return table;
    }

    public boolean isValid() {
        if (isBlank(schema) || isBlank(table))
            return false;
        if (metadata == null || target == null)
            return false;
        if (isBlank(metadata.getBasicSql()))
            return false;
        return metadata.isValid() && target.isValid();
    }

    public TableOptions getOptions() {
        return options;
    }

    public TableRef getTableRef() {
        return tableRef;
    }

    public TableMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(TableMetadata metadata) {
        this.metadata = metadata;
    }

    public boolean isFailure() {
        return failure;
    }

    public void setFailure(boolean failure) {
        this.failure = failure;
    }

    public TargetTable getTarget() {
        return target;
    }

    public void setTarget(TargetTable target) {
        this.target = target;
    }

    public Map<String, TargetTable> getBlobTargets() {
        return blobTargets;
    }

}
