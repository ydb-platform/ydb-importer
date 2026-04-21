package tech.ydb.importer.integration.verification;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.testcontainers.containers.JdbcDatabaseContainer;

import tech.ydb.importer.config.SourceType;
import tech.ydb.importer.integration.sources.mysql.MySqlLoader;
import tech.ydb.importer.integration.sources.mysql.MySqlTestContainer;
import tech.ydb.importer.integration.sources.oracle.OracleLoader;
import tech.ydb.importer.integration.sources.oracle.OracleTestContainer;
import tech.ydb.importer.integration.sources.postgres.PostgresLoader;
import tech.ydb.importer.integration.sources.postgres.PostgresTestContainer;
import tech.ydb.importer.integration.verification.TableScenario.Feature;

/** Source DB test profile with container, connection and dialect loader */
public final class SourceDbProfile {

    public final String name;
    public final JdbcDatabaseContainer<?> container;
    public final SourceType sourceType;
    public final String schema;
    public final DialectLoader loader;
    public final Set<Feature> supported;

    private SourceDbProfile(String name, JdbcDatabaseContainer<?> container,
                            SourceType sourceType, String schema,
                            DialectLoader loader, Set<Feature> supported) {
        this.name = name;
        this.container = container;
        this.sourceType = sourceType;
        this.schema = schema;
        this.loader = loader;
        this.supported = supported;
    }

    public String tableName(TableScenario s) {
        return loader.adaptTableName(s.tableName());
    }

    public Connection openConnection() throws SQLException {
        Connection conn = DriverManager.getConnection(
                container.getJdbcUrl(),
                container.getUsername(),
                container.getPassword());
        loader.onConnectionOpened(conn, schema);
        return conn;
    }

    @Override
    public String toString() {
        return name;
    }

    public static SourceDbProfile mysql(String dbName) {
        return new SourceDbProfile("mysql",
                MySqlTestContainer.create(dbName),
                SourceType.MYSQL, dbName,
                MySqlLoader.INSTANCE,
                EnumSet.of(Feature.BLOB));
    }

    public static SourceDbProfile postgres(String dbName) {
        return new SourceDbProfile("postgres",
                PostgresTestContainer.create(dbName),
                SourceType.POSTGRESQL, "public",
                PostgresLoader.INSTANCE,
                EnumSet.of(Feature.BLOB));
    }

    public static SourceDbProfile oracle() {
        return new SourceDbProfile("oracle",
                OracleTestContainer.create(),
                SourceType.ORACLE, "TEST",
                OracleLoader.INSTANCE,
                EnumSet.of(Feature.BLOB));
    }

    public static List<SourceDbProfile> all(String defaultDbName) {
        List<SourceDbProfile> list = new ArrayList<>();
        list.add(postgres(defaultDbName));
        list.add(mysql(defaultDbName));
        list.add(oracle());
        return list;
    }
}
