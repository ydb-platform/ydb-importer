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
import tech.ydb.importer.integration.sources.clickhouse.ClickHouseLoader;
import tech.ydb.importer.integration.sources.clickhouse.ClickHouseTestContainer;
import tech.ydb.importer.integration.sources.db2.Db2Loader;
import tech.ydb.importer.integration.sources.db2.Db2TestContainer;
import tech.ydb.importer.integration.sources.greenplum.GreenplumTestContainer;
import tech.ydb.importer.integration.sources.hana.HanaLoader;
import tech.ydb.importer.integration.sources.hana.HanaTestContainer;
import tech.ydb.importer.integration.sources.mariadb.MariaDbLoader;
import tech.ydb.importer.integration.sources.mariadb.MariaDbTestContainer;
import tech.ydb.importer.integration.sources.mysql.MySqlLoader;
import tech.ydb.importer.integration.sources.mysql.MySqlTestContainer;
import tech.ydb.importer.integration.sources.oracle.OracleLoader;
import tech.ydb.importer.integration.sources.oracle.OracleTestContainer;
import tech.ydb.importer.integration.sources.postgres.PostgresLoader;
import tech.ydb.importer.integration.sources.postgres.PostgresTestContainer;
import tech.ydb.importer.integration.sources.vertica.VerticaLoader;
import tech.ydb.importer.integration.sources.vertica.VerticaTestContainer;
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

    public static SourceDbProfile clickhouse() {
        return new SourceDbProfile("clickhouse",
                ClickHouseTestContainer.create(),
                SourceType.CLICKHOUSE, "default",
                ClickHouseLoader.INSTANCE,
                EnumSet.of(Feature.PARTITIONED));
    }

    public static SourceDbProfile mariadb(String dbName) {
        return new SourceDbProfile("mariadb",
                MariaDbTestContainer.create(dbName),
                SourceType.MARIADB, dbName,
                MariaDbLoader.INSTANCE,
                EnumSet.of(Feature.BLOB, Feature.PARTITIONED));
    }

    public static SourceDbProfile mysql(String dbName) {
        return new SourceDbProfile("mysql",
                MySqlTestContainer.create(dbName),
                SourceType.MYSQL, dbName,
                MySqlLoader.INSTANCE,
                EnumSet.of(Feature.BLOB, Feature.PARTITIONED));
    }

    public static SourceDbProfile postgres(String dbName) {
        return new SourceDbProfile("postgres",
                PostgresTestContainer.create(dbName),
                SourceType.POSTGRESQL, "public",
                PostgresLoader.INSTANCE,
                EnumSet.of(Feature.BLOB, Feature.PARTITIONED));
    }

    public static SourceDbProfile greenplum() {
        return new SourceDbProfile("greenplum",
                new GreenplumTestContainer(),
                SourceType.GREENPLUM, "public",
                PostgresLoader.INSTANCE,
                EnumSet.of(Feature.PARTITIONED));
    }

    public static SourceDbProfile oracle() {
        return new SourceDbProfile("oracle",
                OracleTestContainer.create(),
                SourceType.ORACLE, "TEST",
                OracleLoader.INSTANCE,
                EnumSet.of(Feature.BLOB, Feature.PARTITIONED));
    }

    public static SourceDbProfile db2() {
        return new SourceDbProfile("db2",
                new Db2TestContainer(),
                SourceType.DB2, Db2TestContainer.SCHEMA,
                Db2Loader.INSTANCE,
                EnumSet.of(Feature.BLOB, Feature.PARTITIONED));
    }

    public static SourceDbProfile vertica() {
        return new SourceDbProfile("vertica",
                new VerticaTestContainer(),
                SourceType.VERTICA, "public",
                VerticaLoader.INSTANCE,
                EnumSet.of(Feature.BLOB, Feature.PARTITIONED));
    }

    public static SourceDbProfile hana() {
        return new SourceDbProfile("hana",
                new HanaTestContainer(),
                SourceType.HANA, HanaTestContainer.SCHEMA,
                HanaLoader.INSTANCE,
                EnumSet.of(Feature.BLOB, Feature.PARTITIONED));
    }

    public static List<SourceDbProfile> all(String defaultDbName) {
        List<SourceDbProfile> list = new ArrayList<>();
        list.add(clickhouse());
        list.add(mariadb(defaultDbName));
        list.add(mysql(defaultDbName));
        list.add(postgres(defaultDbName));
        list.add(greenplum());
        list.add(oracle());
        list.add(db2());
        list.add(vertica());
        list.add(hana());
        return list;
    }
}
