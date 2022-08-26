package ydb.importer.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.jdom2.Element;
import static ydb.importer.config.JdomHelper.*;

/**
 * Direct reference to a single source table, source view,
 * or to an SQL query over the source system.
 * @author zinal
 */
public class TableRef implements TableIdentity {
    
    private TableOptions options;
    private String schemaName;
    private String tableName;
    private String queryText;
    private final List<String> keyNames = new ArrayList<>();
    
    public TableRef() {
    }
    
    public TableRef(Element c, Map<String, TableOptions> optionsMap) {
        this.options = optionsMap.get(getAttr(c, "options"));
        if (this.options == null) {
            throw raiseIllegal(c, "options");
        }
        this.schemaName = getText(c, "schema-name");
        this.tableName = getText(c, "table-name");
        this.queryText = getText(getOneChild(c, "query-text"), true);
        for (Element elKey : getChildren(c, "key-column")) {
            this.keyNames.add(getText(elKey));
        }
    }

    public TableOptions getOptions() {
        return options;
    }

    public void setOptions(TableOptions options) {
        this.options = options;
    }

    @Override
    public String getSchema() {
        return schemaName;
    }

    public void setSchema(String schemaName) {
        this.schemaName = schemaName;
    }

    @Override
    public String getTable() {
        return tableName;
    }

    public void setTable(String tableName) {
        this.tableName = tableName;
    }
    
    public boolean hasQueryText() {
        return ! isBlank(queryText);
    }

    public String getQueryText() {
        return queryText;
    }

    public void setQueryText(String queryText) {
        this.queryText = queryText;
    }

    public List<String> getKeyNames() {
        return keyNames;
    }

}
