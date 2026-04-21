package tech.ydb.importer.integration.sources.mysql;

import org.testcontainers.mysql.MySQLContainer;

public final class MySqlTestContainer {

    public static final String IMAGE = "mysql:8.4.8";

    private MySqlTestContainer() {
    }

    @SuppressWarnings("resource")
    public static MySQLContainer create(String dbName) {
        MySQLContainer c = new MySQLContainer(IMAGE)
                .withDatabaseName(dbName)
                .withUsername("test")
                .withPassword("test");
        c.withUrlParam("rewriteBatchedStatements", "true");
        return c;
    }
}
