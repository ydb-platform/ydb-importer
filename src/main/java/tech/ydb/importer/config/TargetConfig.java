package tech.ydb.importer.config;

import org.jdom2.Element;

/**
 * Target configuration.
 *
 * <p>
 * The target configuration is used to specify the target database connection
 * parameters and processing options.
 * </p>
 *
 * @author zinal
 */
public class TargetConfig extends tech.ydb.importer.config.JdomHelper {

    private TargetType type;
    private YdbAuthMode authMode;
    private String staticLogin;
    private String staticPassword;
    private TargetScript script;
    private String connectionString;
    private String tlsCertificateFile;
    private String saKeyFile;

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
        if (c != null) {
            this.type = TargetType.valueOf(
                    getAttr(c, "type", "ydb").toUpperCase());
            this.authMode = YdbAuthMode.valueOf(
                    getText(c, "auth-mode", "none").toUpperCase());
            if (YdbAuthMode.STATIC == this.authMode) {
                this.staticLogin = getText(c, "static-login");
                this.staticPassword = getText(c, "static-password");
            }
            Element elx = getOneChild(c, "script-file");
            if (elx != null) {
                this.script = new TargetScript(elx);
            }
            elx = getOneChild(c, "connection-string");
            if (elx != null) {
                this.connectionString = getText(elx);
                Element elRepl = getOneChild(c, "replace-existing");
                if (elRepl != null) {
                    this.replaceExisting = parseBoolean(elRepl, null, getText(elRepl));
                }
                Element elData = getOneChild(c, "load-data");
                if (elData != null) {
                    this.loadData = parseBoolean(elData, null, getText(elData));
                }
            }
            elx = getOneChild(c, "tls-certificate-file");
            if (elx != null) {
                this.tlsCertificateFile = getText(elx);
            }
            elx = getOneChild(c, "sa-key-file");
            if (elx != null) {
                this.saKeyFile = getText(elx);
            }
            elx = getOneChild(c, "max-batch-rows");
            if (elx != null) {
                this.maxBatchRows = getInt(elx);
                if (this.maxBatchRows < 0 || this.maxBatchRows > 1000000) {
                    throw raiseIllegal(elx, null, String.valueOf(this.maxBatchRows));
                }
            }
            elx = getOneChild(c, "max-blob-rows");
            if (elx != null) {
                this.maxBlobRows = getInt(elx);
                if (this.maxBlobRows < 0 || this.maxBlobRows > 100000) {
                    throw raiseIllegal(elx, null, String.valueOf(this.maxBlobRows));
                }
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

    public String getStaticLogin() {
        return staticLogin;
    }

    public void setStaticLogin(String staticLogin) {
        this.staticLogin = staticLogin;
    }

    public String getStaticPassword() {
        return staticPassword;
    }

    public void setStaticPassword(String staticPassword) {
        this.staticPassword = staticPassword;
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

    public String getTlsCertificateFile() {
        return tlsCertificateFile;
    }

    public void setTlsCertificateFile(String tlsCertificateFile) {
        this.tlsCertificateFile = tlsCertificateFile;
    }

    public String getSaKeyFile() {
        return saKeyFile;
    }

    public void setSaKeyFile(String saKeyFile) {
        this.saKeyFile = saKeyFile;
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
