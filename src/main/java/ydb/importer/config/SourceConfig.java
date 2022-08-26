package ydb.importer.config;

import org.jdom2.Element;
import static ydb.importer.config.JdomHelper.*;

/**
 *
 * @author zinal
 */
public class SourceConfig {
    
    private SourceType type;
    private String className;
    private String jdbcUrl;
    private String userName;
    private String password;

    public SourceConfig() {
    }

    public SourceConfig(Element c) {
        if (c!=null) {
            this.type = SourceType.valueOf(
                    getAttr(c, "type", "oracle").toUpperCase());
            this.className = getText(c, "jdbc-class");
            this.jdbcUrl = getText(c, "jdbc-url");
            this.userName = getText(c, "username");
            this.password = getText(c, "password");
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
    
}
