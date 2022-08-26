package ydb.importer.config;

import org.jdom2.Element;
import static ydb.importer.config.JdomHelper.*;

/**
 * Options for table name and DDL generator for YDB output.
 * @author zinal
 */
public class TableOptions {

    private final String name;
    private String mainTemplate;
    private String blobTemplate;
    private CaseMode caseMode;
    private DateConv dateConv;

    public TableOptions(String name, String template) {
        this.name = name;
        this.mainTemplate = template;
        this.caseMode = CaseMode.ASIS;
        this.dateConv = DateConv.DATE;
    }

    public TableOptions(Element c) {
        this.name = getAttr(c, "name");
        this.mainTemplate = getText(c, "table-name-format");
        this.blobTemplate = getText(c, "blob-name-format");
        String v;
        v = getText(c, "case-mode", CaseMode.ASIS.name());
        try {
            this.caseMode = CaseMode.valueOf(v.toUpperCase());
        } catch(Exception ex) {
            throw raiseIllegal(c, "case-mode", v);
        }
        v = getText(c, "conv-date", DateConv.DATE.name());
        try {
            this.dateConv = DateConv.valueOf(v.toUpperCase());
        } catch(Exception ex) {
            throw raiseIllegal(c, "conv-date", v);
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

    public static enum CaseMode {
        ASIS,
        UPPER,
        LOWER
    }

    public static enum DateConv {
        DATE,
        STR,
        INT
    }
    
}
