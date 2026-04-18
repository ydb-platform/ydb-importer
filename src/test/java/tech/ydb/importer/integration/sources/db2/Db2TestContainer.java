package tech.ydb.importer.integration.sources.db2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;

import org.testcontainers.db2.Db2Container;

public final class Db2TestContainer extends Db2Container {

    public static final String IMAGE = "icr.io/db2_community/db2:12.1.4.0";
    public static final String SCHEMA = "IMPORT_TEST";

    public Db2TestContainer() {
        super(IMAGE);
        acceptLicense();
        withStartupTimeout(Duration.ofMinutes(15));
    }

    @Override
    public void start() {
        super.start();
        try (Connection c = DriverManager.getConnection(
                getJdbcUrl(), getUsername(), getPassword());
             Statement st = c.createStatement()) {
            st.execute("CREATE SCHEMA " + SCHEMA);
        } catch (SQLException e) {
            throw new RuntimeException(
                    "Failed to create schema " + SCHEMA, e);
        }
    }
}
