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
    private final List<String> clobColumns = new ArrayList<>();
    public static final int AUTO = -1;
    public static final int NONE = -2;
    public static final String SPLIT_BY_AUTO = "auto";
    private String splitBy;
    private String splitFrom;
    private String splitTo;
    private Integer splitCount;
    private Integer ydbPartitionCount;
    private String ydbPartitionFrom;
    private String ydbPartitionTo;
    private Boolean useSourcePartitions;
    private Boolean partitionBuffers;

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
        for (Element elClob : getChildren(c, "clob-column")) {
            this.clobColumns.add(getText(elClob));
        }
        this.splitBy = getText(c, "split-by", null);
        this.splitFrom = parseAutoableText(c, "split-from");
        this.splitTo = parseAutoableText(c, "split-to");
        this.splitCount = parseAutoableCount(c, "split-count", false);
        boolean anySplit = splitBy != null || splitFrom != null
                || splitTo != null || splitCount != null;
        if (anySplit) {
            if (this.splitBy == null) {
                this.splitBy = SPLIT_BY_AUTO;
            }
            if (this.splitCount == null) {
                this.splitCount = AUTO;
            }
        }
        validateSplit(c);

        this.ydbPartitionFrom = parseAutoableText(c, "ydb-partition-from");
        this.ydbPartitionTo = parseAutoableText(c, "ydb-partition-to");
        this.ydbPartitionCount = parseAutoableCount(c, "ydb-partition-count", true);
        validateYdbPartition(c);
        this.useSourcePartitions = parseOptionalBoolean(c, "use-source-partitions");
        this.partitionBuffers = parseOptionalBoolean(c, "use-partition-buffers");
    }

    static Boolean parseOptionalBoolean(Element c, String name) {
        Element el = getOneChild(c, name);
        if (el == null) {
            return null;
        }
        return parseBoolean(el, null, getText(el));
    }

    private static String parseAutoableText(Element c, String name) {
        String value = getText(c, name, null);
        if (value == null) {
            return null;
        }
        if ("auto".equalsIgnoreCase(value.trim())) {
            return null;
        }
        return value;
    }

    static Integer parseAutoableCount(Element c, String name, boolean allowNone) {
        Element el = getOneChild(c, name);
        if (el == null) {
            return null;
        }
        String text = getText(el, true);
        if (text == null || "auto".equalsIgnoreCase(text.trim())) {
            return AUTO;
        }
        if (allowNone && "none".equalsIgnoreCase(text.trim())) {
            return NONE;
        }
        int n;
        try {
            n = Integer.parseInt(text.trim());
        } catch (NumberFormatException ex) {
            throw raiseIllegal(c, name, text);
        }
        if (n < 2) {
            String allowed = allowNone ? "'auto', 'none' or an integer >= 2"
                    : "'auto' or an integer >= 2";
            throw raise(c, name + " must be " + allowed);
        }
        return n;
    }

    private void validateSplit(Element c) {
        if ((splitFrom != null) != (splitTo != null)) {
            throw raise(c, "split-from and split-to must be set together");
        }
    }

    private void validateYdbPartition(Element c) {
        if ((ydbPartitionFrom != null) != (ydbPartitionTo != null)) {
            throw raise(c, "ydb-partition-from and ydb-partition-to must be set together");
        }
        if (ydbPartitionFrom != null && ydbPartitionCount != null
                && (ydbPartitionCount == AUTO || ydbPartitionCount == NONE)) {
            throw raise(c, "ydb-partition-from/to require a numeric ydb-partition-count");
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

    public List<String> getClobColumns() {
        return clobColumns;
    }

    public boolean isClobColumn(String name) {
        return clobColumns.contains(name);
    }

    public boolean hasSplit() {
        return splitBy != null;
    }

    public boolean isSplitByAuto() {
        return SPLIT_BY_AUTO.equalsIgnoreCase(splitBy);
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
        return (splitCount == null || splitCount == AUTO) ? 0 : splitCount;
    }

    public void setSplitCount(int splitCount) {
        this.splitCount = splitCount;
    }

    public boolean isSplitCountAuto() {
        return splitCount != null && splitCount == AUTO;
    }

    public Integer getYdbPartitionCount() {
        return ydbPartitionCount;
    }

    public void setYdbPartitionCount(Integer ydbPartitionCount) {
        this.ydbPartitionCount = ydbPartitionCount;
    }

    public String getYdbPartitionFrom() {
        return ydbPartitionFrom;
    }

    public void setYdbPartitionFrom(String ydbPartitionFrom) {
        this.ydbPartitionFrom = ydbPartitionFrom;
    }

    public String getYdbPartitionTo() {
        return ydbPartitionTo;
    }

    public void setYdbPartitionTo(String ydbPartitionTo) {
        this.ydbPartitionTo = ydbPartitionTo;
    }

    public Boolean getUseSourcePartitions() {
        return useSourcePartitions;
    }

    public void setUseSourcePartitions(Boolean useSourcePartitions) {
        this.useSourcePartitions = useSourcePartitions;
    }

    public Boolean getPartitionBuffers() {
        return partitionBuffers;
    }

    public void setPartitionBuffers(Boolean partitionBuffers) {
        this.partitionBuffers = partitionBuffers;
    }

}
