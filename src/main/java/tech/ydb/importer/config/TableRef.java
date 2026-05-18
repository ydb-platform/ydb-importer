package tech.ydb.importer.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.jdom2.Element;

/**
 * Direct reference to a single source table, source view, or to an SQL query over the source
 * system.
 *
 * @author zinal
 */
public class TableRef extends JdomHelper implements TableIdentity {

    private TableOptions options;
    private String schemaName;
    private String tableName;
    private String queryText;
    private final List<String> keyNames = new ArrayList<>();
    private String splitBy;
    private String splitFrom;
    private String splitTo;
    private int splitCount;

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
        this.splitBy = getText(c, "split-by", null);
        this.splitFrom = getText(c, "split-from", null);
        this.splitTo = getText(c, "split-to", null);
        Element splitCountEl = getOneChild(c, "split-count");
        if (splitCountEl != null) {
            this.splitCount = getInt(splitCountEl);
        }
        boolean anySplit = splitBy != null || splitFrom != null
                || splitTo != null || splitCount > 0;
        boolean allSplit = splitBy != null && splitFrom != null
                && splitTo != null && splitCount >= 2;
        if (anySplit && !allSplit) {
            throw raise(c, "split-by, split-from, split-to, "
                    + "split-count (>= 2) must all be specified together");
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
        return !isBlank(queryText);
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

    public boolean hasSplit() {
        return splitBy != null;
    }

    public String getSplitBy() {
        return splitBy;
    }

    public void setSplitBy(String splitBy) {
        this.splitBy = splitBy;
    }

    public String getSplitFrom() {
        return splitFrom;
    }

    public void setSplitFrom(String splitFrom) {
        this.splitFrom = splitFrom;
    }

    public String getSplitTo() {
        return splitTo;
    }

    public void setSplitTo(String splitTo) {
        this.splitTo = splitTo;
    }

    public int getSplitCount() {
        return splitCount;
    }

    public void setSplitCount(int splitCount) {
        this.splitCount = splitCount;
    }

}
