package tech.ydb.importer.integration.sources.postgres;

import java.util.TimeZone;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.testcontainers.postgresql.PostgreSQLContainer;

import tech.ydb.importer.config.SourceType;

/** PostgreSQL integration tests. */
public class PostgresYdbImporterTest {

    private static PostgreSQLContainer pgContainer;
    private static TimeZone originalTz;

    @BeforeAll
    static void startPostgres() {
        // Pin JVM to UTC so PG TIMESTAMP values round-trip without offset.
        originalTz = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        pgContainer = PostgresTestContainer.create("import_test");
        pgContainer.start();
    }

    @AfterAll
    static void stopPostgres() {
        if (pgContainer != null) {
            pgContainer.stop();
            pgContainer = null;
        }
        if (originalTz != null) {
            TimeZone.setDefault(originalTz);
        }
    }

    @Nested
    class TypeTests extends AbstractPostgresCompatibleTypeCases {
        @Override
        public SourceDb sourceDb() {
            return new SourceDb(pgContainer, SourceType.POSTGRESQL, "public");
        }
    }

    @Nested
    class TableTests extends AbstractPostgresCompatibleTableCases {
        @Override
        public SourceDb sourceDb() {
            return new SourceDb(pgContainer, SourceType.POSTGRESQL, "public");
        }
    }
}
