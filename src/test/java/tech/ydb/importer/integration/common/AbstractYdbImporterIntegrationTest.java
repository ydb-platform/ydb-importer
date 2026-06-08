package tech.ydb.importer.integration.common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.testcontainers.containers.JdbcDatabaseContainer;

import tech.ydb.importer.config.SourceType;

/** Shared base for integration tests that run the importer end-to-end */
public abstract class AbstractYdbImporterIntegrationTest {

    public static final class SourceDb {
        public final JdbcDatabaseContainer<?> container;
        public final SourceType type;
        public final String schema;

        public SourceDb(JdbcDatabaseContainer<?> container, SourceType type, String schema) {
            this.container = container;
            this.type = type;
            this.schema = schema;
        }
    }

    private static LocalYdbTestContainer ydb;

    public abstract SourceDb sourceDb();

    public JdbcDatabaseContainer<?> sourceContainer() {
        return sourceDb().container;
    }

    public SourceType sourceType() {
        return sourceDb().type;
    }

    public String schemaName() {
        return sourceDb().schema;
    }

    public LocalYdbTestContainer ydbContainer() {
        if (ydb == null) {
            ydb = new LocalYdbTestContainer();
            ydb.start();
        }
        return ydb;
    }

    public boolean useArrow() {
        return false;
    }

    public Connection openSourceConnection() throws Exception {
        JdbcDatabaseContainer<?> container = sourceContainer();
        Class.forName(container.getDriverClassName());
        return DriverManager.getConnection(
                container.getJdbcUrl(),
                container.getUsername(),
                container.getPassword());
    }

    public void executeOnSource(String... statements) throws Exception {
        try (Connection con = openSourceConnection();
             Statement st = con.createStatement()) {
            for (String sql : statements) {
                st.execute(sql);
            }
        }
    }
}
