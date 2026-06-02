package tech.ydb.importer.config;

import org.jdom2.Element;

/**
 * Source configuration.
 *
 * <p>
 * The source configuration is used to specify the source database connection
 * parameters.
 * </p>
 *
 * @author zinal
 */
public class SourceConfig extends JdomHelper {

    private SourceType type;
    private String className;
    private String jdbcUrl;
    private String userName;
    private String password;
    private int fetchSize = 10000;
    private int retryCount = 10;

    public SourceConfig() {
    }

    public SourceConfig(Element c) {
        if (c != null) {
            this.type = SourceType.valueOf(
                    getAttr(c, "type", "oracle").toUpperCase());
            this.className = getText(c, "jdbc-class");
            this.jdbcUrl = getText(c, "jdbc-url");
            this.userName = getText(c, "username");
            this.password = getText(c, "password");
            Element fetchSizeEl = getOneChild(c, "fetch-size");
            if (fetchSizeEl != null) {
                int fs = getInt(fetchSizeEl);
                if (fs < 0) {
                    throw raiseIllegal(fetchSizeEl, null, String.valueOf(fs));
                }
                this.fetchSize = fs;
            }
            Element retryCountEl = getOneChild(c, "retry-count");
            if (retryCountEl != null) {
                int rc = getInt(retryCountEl);
                if (rc < 0) {
                    throw raiseIllegal(retryCountEl, null, String.valueOf(rc));
                }
                this.retryCount = rc;
            }
        }
    }

    public SourceType getType() {
        return type;
    }

    public void setType(SourceType type) {
        this.type = type;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

}
