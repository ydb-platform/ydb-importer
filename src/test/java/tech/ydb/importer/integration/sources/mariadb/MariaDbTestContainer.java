package tech.ydb.importer.integration.sources.mariadb;

import org.testcontainers.mariadb.MariaDBContainer;

public final class MariaDbTestContainer {

    public static final String IMAGE = "mariadb:11.8.6";

    private MariaDbTestContainer() {
    }

    @SuppressWarnings("resource")
    public static MariaDBContainer create(String dbName) {
        return new MariaDBContainer(IMAGE)
                .withDatabaseName(dbName)
                .withUsername("test")
                .withPassword("test");
    }
}
