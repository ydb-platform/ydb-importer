package tech.ydb.importer.integration.sources.hana;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.utility.DockerImageName;

public final class HanaTestContainer
        extends JdbcDatabaseContainer<HanaTestContainer> {

    public static final String DEFAULT_IMAGE =
            "saplabs/hanaexpress:2.00.088.00.20251110.1";
    public static final String SCHEMA = "IMPORT_TEST";

    private static final int SYSTEMDB_PORT = 39013;
    private static final int TENANT_PORT = 39041;
    private static final String MASTER_PASSWORD = "HxeTest1";

    public HanaTestContainer() {
        this(DEFAULT_IMAGE);
    }

    public HanaTestContainer(String imageName) {
        super(DockerImageName.parse(imageName));
        addExposedPort(SYSTEMDB_PORT);
        addExposedPort(TENANT_PORT);
        withStartupTimeoutSeconds(900);
        withConnectTimeoutSeconds(300);
        withCommand(
                "--agree-to-sap-license",
                "--master-password", MASTER_PASSWORD);
        // Mount /hana/mounts as tmpfs to speed up HANA startup.
        withCreateContainerCmdModifier(cmd -> cmd.getHostConfig()
                .withTmpFs(Collections.singletonMap("/hana/mounts",
                        "rw,exec,mode=0770,uid=12000,gid=79,size=32g")));
    }

    @Override
    public String getDriverClassName() {
        return "com.sap.db.jdbc.Driver";
    }

    @Override
    public String getJdbcUrl() {
        return "jdbc:sap://" + getHost() + ":" + getMappedPort(TENANT_PORT);
    }

    @Override
    public String getUsername() {
        return "SYSTEM";
    }

    @Override
    public String getPassword() {
        return MASTER_PASSWORD;
    }

    @Override
    protected String getTestQueryString() {
        return "SELECT 1 FROM DUMMY";
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
