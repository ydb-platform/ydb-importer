package tech.ydb.importer.integration.sources.clickhouse;

import org.testcontainers.clickhouse.ClickHouseContainer;

public final class ClickHouseTestContainer {

    public static final String IMAGE = "clickhouse/clickhouse-server:25.3.10.19";

    private ClickHouseTestContainer() {
    }

    public static ClickHouseContainer create() {
        return new ClickHouseContainer(IMAGE);
    }
}
