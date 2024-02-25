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
    private boolean skipUnknownTypes;

    public TableOptions(String name, String template) {
        this.name = name;
        this.mainTemplate = template;
        this.caseMode = CaseMode.ASIS;
        this.dateConv = DateConv.DATE;
        this.timestampConv = DateConv.DATE;
        this.skipUnknownTypes = false;
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
        v = getText(c, "conv-date", DateConv.DATE.name());
        try {
            this.dateConv = DateConv.valueOf(v.toUpperCase());
        } catch (Exception ex) {
            throw raiseIllegal(c, "conv-date", v);
        }
        v = getText(c, "conv-timestamp", DateConv.DATE.name());
        try {
            this.timestampConv = DateConv.valueOf(v.toUpperCase());
        } catch (Exception ex) {
            throw raiseIllegal(c, "conv-timestamp", v);
        }
        v = getText(c, "skip-unknown-types", "false");
        try {
            this.skipUnknownTypes = Boolean.parseBoolean(v);
        } catch (Exception ex) {
            throw raiseIllegal(c, "skip-unknown-types", v);
        }
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

    public boolean isSkipUnknownTypes() {
        return skipUnknownTypes;
    }

    public void setSkipUnknownTypes(boolean skipUnknownTypes) {
        this.skipUnknownTypes = skipUnknownTypes;
    }

    public enum CaseMode {
        ASIS,
        UPPER,
        LOWER
    }

    public enum DateConv {
        DATE,
        STR,
        INT
    }

}
