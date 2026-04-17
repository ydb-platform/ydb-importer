package tech.ydb.importer.integration.sources.postgres;

import org.testcontainers.postgresql.PostgreSQLContainer;

public final class PostgresTestContainer {

    public static final String IMAGE = "postgres:17.5";

    private PostgresTestContainer() {
    }

    @SuppressWarnings("resource")
    public static PostgreSQLContainer create(String dbName) {
        return new PostgreSQLContainer(IMAGE)
                .withDatabaseName(dbName)
                .withUsername("test")
                .withPassword("test");
    }
}
