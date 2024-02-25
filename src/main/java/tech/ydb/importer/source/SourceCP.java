package tech.ydb.importer.source;

import java.sql.Connection;
import java.sql.SQLException;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import tech.ydb.importer.config.SourceConfig;

/**
 * Connection pool wrapper object
 *
 * @author zinal
 */
public class SourceCP implements AutoCloseable {

    private final HikariDataSource ds;

    public SourceCP(SourceConfig sc, int poolSize) throws SQLException {
        HikariConfig hc = new HikariConfig();
        hc.setAutoCommit(false);
        hc.setJdbcUrl(sc.getJdbcUrl());
        hc.setUsername(sc.getUserName());
        hc.setPassword(sc.getPassword());
        hc.setMaximumPoolSize(poolSize);
        this.ds = new HikariDataSource(hc);
    }

    public Connection getConnection() throws SQLException {
        return ds.getConnection();
    }

    @Override
    public void close() {
        ds.close();
    }

}
