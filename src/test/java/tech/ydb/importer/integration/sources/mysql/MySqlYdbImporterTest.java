package tech.ydb.importer.integration.sources.mysql;

import java.util.TimeZone;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.testcontainers.mysql.MySQLContainer;

import tech.ydb.importer.config.SourceType;

/** MySQL integration tests. */
public class MySqlYdbImporterTest {

    private static final String DB_NAME = "import_test";

    private static MySQLContainer mysqlContainer;
    private static TimeZone originalTz;

    @BeforeAll
    static void startMySql() {
        originalTz = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        mysqlContainer = MySqlTestContainer.create(DB_NAME);
        mysqlContainer.start();
    }

    @AfterAll
    static void stopMySql() {
        if (mysqlContainer != null) {
            mysqlContainer.stop();
            mysqlContainer = null;
        }
        if (originalTz != null) {
            TimeZone.setDefault(originalTz);
        }
    }

    abstract class MySqlTypeCases extends AbstractMySqlCompatibleTypeCases {
        @Override
        public SourceDb sourceDb() {
            return new SourceDb(mysqlContainer, SourceType.MYSQL, DB_NAME);
        }
    }

    @Nested class TypeTestsRow extends MySqlTypeCases {
        @Override public boolean useArrow() { return false; }
    }

    @Nested class TypeTestsArrow extends MySqlTypeCases {
        @Override public boolean useArrow() { return true; }
    }

    abstract class MySqlTableCases extends AbstractMySqlCompatibleTableCases {
        @Override
        public SourceDb sourceDb() {
            return new SourceDb(mysqlContainer, SourceType.MYSQL, DB_NAME);
        }
    }

    @Nested
    class PartitioningTests extends AbstractMySqlPartitioningTests {
        @Override
        public SourceDb sourceDb() {
            return new SourceDb(mysqlContainer, SourceType.MYSQL, DB_NAME);
        }
    }

    @Nested class TableTestsRow extends MySqlTableCases {
        @Override public boolean useArrow() { return false; }
    }

    @Nested class TableTestsArrow extends MySqlTableCases {
        @Override public boolean useArrow() { return true; }
    }
}
