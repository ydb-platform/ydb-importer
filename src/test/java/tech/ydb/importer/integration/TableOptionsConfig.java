package tech.ydb.importer.integration;

import tech.ydb.importer.config.TableOptions;

/**
 * Table-options config description.
 */
public final class TableOptionsConfig {

    private final String name;
    private final String tableNameFormat;
    private final String blobNameFormat;
    private final TableOptions.CaseMode caseMode;
    private final TableOptions.DateConv convDate;
    private final TableOptions.DateConv convTimestamp;
    private final boolean allowCustomDecimal;
    private final boolean skipUnknownTypes;

    public TableOptionsConfig(
            String name,
            String tableNameFormat,
            String blobNameFormat,
            TableOptions.CaseMode caseMode,
            TableOptions.DateConv convDate,
            TableOptions.DateConv convTimestamp,
            boolean allowCustomDecimal,
            boolean skipUnknownTypes) {
        this.name = name;
        this.tableNameFormat = tableNameFormat;
        this.blobNameFormat = blobNameFormat;
        this.caseMode = caseMode;
        this.convDate = convDate;
        this.convTimestamp = convTimestamp;
        this.allowCustomDecimal = allowCustomDecimal;
        this.skipUnknownTypes = skipUnknownTypes;
    }

    public String getName() {
        return name;
    }

    public String getTableNameFormat() {
        return tableNameFormat;
    }

    public String getBlobNameFormat() {
        return blobNameFormat;
    }

    public TableOptions.CaseMode getCaseMode() {
        return caseMode;
    }

    public TableOptions.DateConv getConvDate() {
        return convDate;
    }

    public TableOptions.DateConv getConvTimestamp() {
        return convTimestamp;
    }

    public boolean isAllowCustomDecimal() {
        return allowCustomDecimal;
    }

    public boolean isSkipUnknownTypes() {
        return skipUnknownTypes;
    }
}
