package tech.ydb.importer.integration.common;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/** Testcontainers wrapper for a local YDB instance */
public class LocalYdbTestContainer extends GenericContainer<LocalYdbTestContainer> {

    private static final int TEST_GRPC_PORT = 52137;

    public LocalYdbTestContainer() {
        super(DockerImageName.parse("ydbplatform/local-ydb:26.1"));

        withEnv("GRPC_PORT", String.valueOf(TEST_GRPC_PORT));
        withEnv("YDB_USE_IN_MEMORY_PDISKS", "true");

        addFixedExposedPort(TEST_GRPC_PORT, TEST_GRPC_PORT);

        withCreateContainerCmdModifier(cmd -> cmd.withHostName("localhost"));

        waitingFor(Wait.forHealthcheck());
    }

    public String getConnectionString() {
        return "grpc://localhost:" + TEST_GRPC_PORT + "/local";
    }
}
