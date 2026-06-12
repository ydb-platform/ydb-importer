package tech.ydb.importer.integration.sources.greenplum;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.utility.DockerImageName;

public final class GreenplumTestContainer
        extends JdbcDatabaseContainer<GreenplumTestContainer> {

    public static final String DEFAULT_IMAGE = "woblerr/greenplum:7.1.0";

    private static final int GP_PORT = 5432;

    public GreenplumTestContainer() {
        this(DEFAULT_IMAGE);
    }

    public GreenplumTestContainer(String imageName) {
        super(DockerImageName.parse(imageName));
        addExposedPort(GP_PORT);
        withEnv("GREENPLUM_PASSWORD", "test");
        withStartupTimeoutSeconds(300);
        withConnectTimeoutSeconds(120);
    }

    @Override
    public String getDriverClassName() {
        return "org.postgresql.Driver";
    }

    @Override
    public String getJdbcUrl() {
        return "jdbc:postgresql://" + getHost() + ":"
                + getMappedPort(GP_PORT) + "/demo";
    }

    @Override
    public String getUsername() {
        return "gpadmin";
    }

    @Override
    public String getPassword() {
        return "test";
    }

    @Override
    protected String getTestQueryString() {
        return "SELECT 1";
    }
}
