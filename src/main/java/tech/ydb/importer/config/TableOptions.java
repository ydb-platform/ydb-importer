package tech.ydb.importer.config;

import org.jdom2.Element;

/**
 * Options for table name and DDL generator for YDB output.
 *
 * @author zinal
 */
public class TableOptions extends JdomHelper {

    private final String name;
    private String mainTemplate;
    private String blobTemplate;
    private CaseMode caseMode;
    private DateConv dateConv;
    private DateConv timestampConv;
    private boolean allowCustomDecimal;
    private boolean skipUnknownTypes;
    private StoreType storeType;
    private Boolean useSourcePartitions;
    private Integer ydbPartitionCount;
    private Boolean partitionBuffers;

    public TableOptions(String name, String template) {
        this.name = name;
        this.mainTemplate = template;
        this.caseMode = CaseMode.ASIS;
        this.dateConv = DateConv.DATE_NEW;
        this.timestampConv = DateConv.DATE_NEW;
        this.allowCustomDecimal = true;
        this.skipUnknownTypes = false;
        this.storeType = StoreType.ROW;
    }

    public TableOptions(Element c) {
        this.name = getAttr(c, "name");
        this.mainTemplate = getText(c, "table-name-format");
        this.blobTemplate = getText(c, "blob-name-format");
        String v;
        v = getText(c, "case-mode", CaseMode.ASIS.name());
        try {
            this.caseMode = CaseMode.valueOf(v.toUpperCase());
        } catch (Exception ex) {
            throw raiseIllegal(c, "case-mode", v);
        }
        v = getText(c, "conv-date", DateConv.DATE_NEW.name());
        try {
            this.dateConv = DateConv.valueOf(v.toUpperCase());
        } catch (Exception ex) {
            throw raiseIllegal(c, "conv-date", v);
        }
        v = getText(c, "conv-timestamp", DateConv.DATE_NEW.name());
        try {
            this.timestampConv = DateConv.valueOf(v.toUpperCase());
        } catch (Exception ex) {
            throw raiseIllegal(c, "conv-timestamp", v);
        }
        v = getText(c, "allow-custom-decimal", "true");
        this.allowCustomDecimal = Boolean.parseBoolean(v);
        v = getText(c, "skip-unknown-types", "false");
        this.skipUnknownTypes = Boolean.parseBoolean(v);
        v = getText(c, "store-type", StoreType.ROW.name());
        try {
            this.storeType = StoreType.valueOf(v.toUpperCase());
        } catch (Exception ex) {
            throw raiseIllegal(c, "store-type", v);
        }
        this.useSourcePartitions = TableRef.parseOptionalBoolean(c, "use-source-partitions");
        this.ydbPartitionCount = TableRef.parseAutoableCount(c, "ydb-partition-count", true);
        this.partitionBuffers = TableRef.parseOptionalBoolean(c, "use-partition-buffers");
    }

    public String getName() {
        return name;
    }

    public String getMainTemplate() {
        return mainTemplate;
    }

    public void setMainTemplate(String template) {
        this.mainTemplate = template;
    }

    public String getBlobTemplate() {
        return blobTemplate;
    }

    public void setBlobTemplate(String blobTemplate) {
        this.blobTemplate = blobTemplate;
    }

    public CaseMode getCaseMode() {
        return caseMode;
    }

    public void setCaseMode(CaseMode caseMode) {
        this.caseMode = caseMode;
    }

    public DateConv getDateConv() {
        return dateConv;
    }

    public void setDateConv(DateConv dateConv) {
        this.dateConv = dateConv;
    }

    public DateConv getTimestampConv() {
        return timestampConv;
    }

    public void setTimestampConv(DateConv timestampConv) {
        this.timestampConv = timestampConv;
    }

    public boolean isAllowCustomDecimal() {
        return allowCustomDecimal;
    }

    public void setAllowCustomDecimal(boolean allowCustomDecimal) {
        this.allowCustomDecimal = allowCustomDecimal;
    }

    public boolean isSkipUnknownTypes() {
        return skipUnknownTypes;
    }

    public void setSkipUnknownTypes(boolean skipUnknownTypes) {
        this.skipUnknownTypes = skipUnknownTypes;
    }

    public StoreType getStoreType() {
        return storeType;
    }

    public void setStoreType(StoreType storeType) {
        this.storeType = storeType;
    }

    public Boolean getUseSourcePartitions() {
        return useSourcePartitions;
    }

    public void setUseSourcePartitions(Boolean useSourcePartitions) {
        this.useSourcePartitions = useSourcePartitions;
    }

    public Integer getYdbPartitionCount() {
        return ydbPartitionCount;
    }

    public void setYdbPartitionCount(Integer ydbPartitionCount) {
        this.ydbPartitionCount = ydbPartitionCount;
    }

    public Boolean getPartitionBuffers() {
        return partitionBuffers;
    }

    public void setPartitionBuffers(Boolean partitionBuffers) {
        this.partitionBuffers = partitionBuffers;
    }

    /**
     * The store type to be used by the target tables.
     */
    public enum StoreType {
        /**
         * Row organized storage.
         */
        ROW,
        /**
         * Column organized storage.
         */
        COLUMN
    }

    /**
     * Case mode for table name.
     */
    public enum CaseMode {
        /**
         * Leave table name as is.
         */
        ASIS,
        /**
         * Convert table name to upper case.
         */
        UPPER,
        /**
         * Convert table name to lower case.
         */
        LOWER
    }

    /**
     * Date conversion mode.
     */
    public enum DateConv {
        /**
         * Date in YDB format, new Date32, Datetime64 and Timestamp64 data
         * types.
         */
        DATE_NEW,
        /**
         * Date in YDB format, classical Date, Datetime and Timestamp data
         * types.
         */
        DATE,
        /**
         * Date or time as string, using formats "YYYY-MM-DD" and
         * "YYYY-MM-DDTHH:MM:SS[.SSSSSSSSS]Z".
         */
        STR,
        /**
         * Date or time as integer, using format "YYYYMMDD" for dates and
         * epoch-based milliseconds for timestamps.
         */
        INT
    }

}
