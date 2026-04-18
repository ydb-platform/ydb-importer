package tech.ydb.importer.integration.sources.oracle;

import org.testcontainers.oracle.OracleContainer;

public final class OracleTestContainer {

    public static final String IMAGE = "gvenzl/oracle-free:23-full";

    private OracleTestContainer() {
    }

    @SuppressWarnings("resource")
    public static OracleContainer create() {
        return new OracleContainer(IMAGE)
                .withStartupTimeoutSeconds(600);
    }
}
