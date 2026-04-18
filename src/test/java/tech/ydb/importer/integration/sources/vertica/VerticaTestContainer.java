package tech.ydb.importer.integration.sources.vertica;

import java.util.Collections;

import com.github.dockerjava.api.model.Ulimit;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.utility.DockerImageName;

public final class VerticaTestContainer
        extends JdbcDatabaseContainer<VerticaTestContainer> {

    public static final String DEFAULT_IMAGE = "vertica-ce-bench:latest";

    private static final int VERTICA_PORT = 5433;

    public VerticaTestContainer() {
        this(DEFAULT_IMAGE);
    }

    public VerticaTestContainer(String imageName) {
        super(DockerImageName.parse(imageName));
        addExposedPort(VERTICA_PORT);
        withStartupTimeoutSeconds(300);
        withConnectTimeoutSeconds(120);
        withCreateContainerCmdModifier(cmd ->
                cmd.getHostConfig().withUlimits(
                        Collections.singletonList(
                                new Ulimit("nofile", 65536L, 65536L))));
    }

    @Override
    public String getDriverClassName() {
        return "com.vertica.jdbc.Driver";
    }

    @Override
    public String getJdbcUrl() {
        return "jdbc:vertica://" + getHost() + ":"
                + getMappedPort(VERTICA_PORT) + "/VMart";
    }

    @Override
    public String getUsername() {
        return "dbadmin";
    }

    @Override
    public String getPassword() {
        return "";
    }

    @Override
    protected String getTestQueryString() {
        return "SELECT 1";
    }
}
