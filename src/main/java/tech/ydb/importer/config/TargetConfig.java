package tech.ydb.importer.config;

import org.jdom2.Element;
import static tech.ydb.importer.config.JdomHelper.*;

/**
 *
 * @author zinal
 */
public class TargetConfig {
    
    private TargetType type;
    private YdbAuthMode authMode;
    private TargetScript script;
    private String connectionString;
    private boolean replaceExisting;
    private boolean loadData;
    private int maxBatchRows;
    private int maxBlobRows;

    public TargetConfig() {
        this.type = TargetType.YDB;
        this.authMode = YdbAuthMode.NONE;
        this.replaceExisting = false;
        this.loadData = false;
        this.maxBatchRows = 100;
        this.maxBlobRows = 100;
    }
    
    public TargetConfig(Element c) {
        this.type = TargetType.YDB;
        this.authMode = YdbAuthMode.NONE;
        this.replaceExisting = false;
        this.loadData = false;
        this.maxBatchRows = 100;
        this.maxBlobRows = 100;
        if (c!=null) {
            this.type = TargetType.valueOf(
                    getAttr(c, "type", "ydb").toUpperCase());
            this.authMode = YdbAuthMode.valueOf(
                    getText(c, "auth-mode", "none").toUpperCase());
            Element elx = getOneChild(c, "script-file");
            if (elx != null) {
                this.script = new TargetScript(elx);
            }
            elx = getOneChild(c, "connection-string");
            if (elx != null) {
                this.connectionString = getText(elx);
                Element elRepl = getOneChild(c, "replace-existing");
                if (elRepl!=null)
                    this.replaceExisting = parseBoolean(elRepl, null, getText(elRepl));
                Element elData = getOneChild(c, "load-data");
                if (elData!=null)
                    this.loadData = parseBoolean(elData, null, getText(elData));
            }
            elx = getOneChild(c, "max-batch-rows");
            if (elx != null) {
                this.maxBatchRows = getInt(elx);
                if (this.maxBatchRows < 0 || this.maxBatchRows > 1000000)
                    throw raiseIllegal(elx, null, String.valueOf(this.maxBatchRows));
            }
            elx = getOneChild(c, "max-blob-rows");
            if (elx != null) {
                this.maxBlobRows = getInt(elx);
                if (this.maxBlobRows < 0 || this.maxBlobRows > 100000)
                    throw raiseIllegal(elx, null, String.valueOf(this.maxBlobRows));
            }
        }
    }

    public TargetType getType() {
        return type;
    }

    public void setType(TargetType type) {
        this.type = type;
    }

    public YdbAuthMode getAuthMode() {
        return authMode;
    }

    public void setAuthMode(YdbAuthMode authMode) {
        this.authMode = authMode;
    }

    public TargetScript getScript() {
        return script;
    }

    public void setScript(TargetScript script) {
        this.script = script;
    }

    public String getConnectionString() {
        return connectionString;
    }

    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }

    public boolean isReplaceExisting() {
        return replaceExisting;
    }

    public void setReplaceExisting(boolean replaceExisting) {
        this.replaceExisting = replaceExisting;
    }

    public boolean isLoadData() {
        return loadData;
    }

    public void setLoadData(boolean loadData) {
        this.loadData = loadData;
    }

    public int getMaxBatchRows() {
        return maxBatchRows;
    }

    public void setMaxBatchRows(int maxBatchRows) {
        this.maxBatchRows = maxBatchRows;
    }

    public int getMaxBlobRows() {
        return maxBlobRows;
    }

    public void setMaxBlobRows(int maxBlobRows) {
        this.maxBlobRows = maxBlobRows;
    }

}
