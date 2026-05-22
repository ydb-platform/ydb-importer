package tech.ydb.importer.integration.sources.mariadb.columnstore;

import java.time.Duration;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.mariadb.MariaDBContainer;
import org.testcontainers.utility.DockerImageName;

public final class ColumnStoreContainer {

    public static final String DEFAULT_IMAGE = "mariadb/columnstore:23.02.4";
    public static final String DEFAULT_DB_NAME = "cs_test";
    public static final String DEFAULT_USERNAME = "csuser";
    public static final String DEFAULT_PASSWORD = "C0lumnStore!";

    private static final int MYSQL_PORT = 3306;

    private final String imageName;
    private final String dbName;
    private final String userName;
    private final String password;
    private final GenericContainer<?> container;
    private MariaDBContainer jdbcView;

    public ColumnStoreContainer() {
        this(DEFAULT_IMAGE, DEFAULT_DB_NAME, DEFAULT_USERNAME, DEFAULT_PASSWORD);
    }

    @SuppressWarnings("resource")
    public ColumnStoreContainer(String imageName, String dbName,
                                String userName, String password) {
        this.imageName = imageName;
        this.dbName = dbName;
        this.userName = userName;
        this.password = password;
        this.container = new GenericContainer<>(imageName)
                .withExposedPorts(MYSQL_PORT)
                .withSharedMemorySize(512 * 1024 * 1024L)
                .withEnv("PM1", "mcs1")
                .withEnv("ADMIN_HOST", "%")
                .withEnv("ADMIN_PASS", password)
                .withCreateContainerCmdModifier(
                        cmd -> cmd.withHostName("mcs1"))
                .waitingFor(Wait.forLogMessage(
                        ".*ready for connections.*", 1))
                .withStartupTimeout(Duration.ofMinutes(3));
    }

    public void start() throws Exception {
        container.start();
        container.execInContainer("provision", "mcs1");
        container.execInContainer("mariadb", "-uroot", "-p" + password, "-e",
                "CREATE DATABASE IF NOT EXISTS " + dbName + ";"
                + "CREATE USER IF NOT EXISTS '" + userName
                + "'@'%' IDENTIFIED BY '" + password + "';"
                + "GRANT ALL PRIVILEGES ON *.* TO '"
                + userName + "'@'%'; FLUSH PRIVILEGES;");
        jdbcView = buildJdbcView();
    }

    public void stop() {
        container.stop();
        jdbcView = null;
    }

    public MariaDBContainer getJdbcContainer() {
        if (jdbcView == null) {
            throw new IllegalStateException(
                    "ColumnStoreContainer not started");
        }
        return jdbcView;
    }

    public String getDatabaseName() {
        return dbName;
    }

    private MariaDBContainer buildJdbcView() {
        final String jdbcUrl = "jdbc:mariadb://localhost:"
                + container.getMappedPort(MYSQL_PORT) + "/" + dbName;
        DockerImageName image = DockerImageName.parse(imageName)
                .asCompatibleSubstituteFor("mariadb");
        return new MariaDBContainer(image) {
            @Override public String getJdbcUrl() { return jdbcUrl; }
            @Override public String getUsername() { return userName; }
            @Override public String getPassword() { return password; }
            @Override public String getDriverClassName() {
                return "org.mariadb.jdbc.Driver";
            }
        };
    }
}
