package tech.ydb.importer.integration.sources.mariadb;

import java.util.TimeZone;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.testcontainers.mariadb.MariaDBContainer;

import tech.ydb.importer.config.SourceType;
import tech.ydb.importer.integration.sources.mysql.AbstractMySqlCompatibleTableCases;
import tech.ydb.importer.integration.sources.mysql.AbstractMySqlCompatibleTypeCases;

/** MariaDB integration tests. */
public class MariaDbYdbImporterTest {

    private static final String DB_NAME = "import_test";

    private static MariaDBContainer mariaContainer;
    private static TimeZone originalTz;

    @BeforeAll
    static void startMariaDb() {
        originalTz = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        mariaContainer = MariaDbTestContainer.create(DB_NAME);
        mariaContainer.start();
    }

    @AfterAll
    static void stopMariaDb() {
        if (mariaContainer != null) {
            mariaContainer.stop();
            mariaContainer = null;
        }
        if (originalTz != null) {
            TimeZone.setDefault(originalTz);
        }
    }

    abstract class MariaDbTypeCases
            extends AbstractMySqlCompatibleTypeCases {
        @Override
        public SourceDb sourceDb() {
            return new SourceDb(mariaContainer, SourceType.MARIADB, DB_NAME);
        }
    }

    @Nested class TypeTestsRow extends MariaDbTypeCases {
        @Override public boolean useArrow() { return false; }
    }

    @Nested class TypeTestsArrow extends MariaDbTypeCases {
        @Override public boolean useArrow() { return true; }
    }

    abstract class MariaDbTableCases
            extends AbstractMySqlCompatibleTableCases {
        @Override
        public SourceDb sourceDb() {
            return new SourceDb(mariaContainer, SourceType.MARIADB, DB_NAME);
        }

        @Test
        public void skipUnsupportedUuidColumn() throws Exception {
            String s = schemaName();
            tableTest(s, "skip_uuid")
                    
                    .withOptions(opts -> opts.setSkipUnknownTypes(true))
                    .setupSql(
                        "CREATE TABLE " + s + ".skip_uuid ("
                        + "id INT NOT NULL PRIMARY KEY,"
                        + "name VARCHAR(50) NOT NULL,"
                        + "uid UUID"
                        + ") ENGINE=InnoDB;"
                        + "INSERT INTO " + s + ".skip_uuid VALUES"
                        + " (1,'a',UUID()),"
                        + "(2,'b',UUID())")
                    .cleanupSql("DROP TABLE IF EXISTS "
                            + s + ".skip_uuid")
                    .expectPrimaryKey("id")
                    .expectSkippedColumns("uid")
                    .expectRowCount(2)
                    .expectRowExists("id", 1, "name", "a")
                    .run();
        }

        @Test
        public void rangePartitioning() throws Exception {
            String s = schemaName();
            tableTest(s, "part_range")
                    
                    .setupSql(
                        "CREATE TABLE " + s + ".part_range ("
                        + "id INT NOT NULL,"
                        + "region_id INT NOT NULL,"
                        + "amount INT NOT NULL,"
                        + "PRIMARY KEY (id, region_id)"
                        + ") ENGINE=InnoDB"
                        + " PARTITION BY RANGE (region_id) ("
                        + "  PARTITION p_low VALUES LESS THAN (100),"
                        + "  PARTITION p_mid VALUES LESS THAN (200),"
                        + "  PARTITION p_high VALUES LESS THAN MAXVALUE"
                        + ");"
                        + "INSERT INTO " + s + ".part_range VALUES"
                        + " (1,10,100),(2,50,200),"
                        + "(3,110,300),(4,250,400)")
                    .cleanupSql("DROP TABLE IF EXISTS "
                            + s + ".part_range")
                    .expectPrimaryKey("id", "region_id")
                    .expectRowCount(4)
                    .expectRowExists("id", 1, "amount", 100)
                    .expectRowExists("id", 4, "amount", 400)
                    .run();
        }

        @Test
        public void singlePartitionNotPartitioned() throws Exception {
            String s = schemaName();
            tableTest(s, "part_single")
                    
                    .setupSql(
                        "CREATE TABLE " + s + ".part_single ("
                        + "id INT NOT NULL PRIMARY KEY"
                        + ") ENGINE=InnoDB"
                        + " PARTITION BY RANGE (id) ("
                        + "  PARTITION p_all VALUES LESS THAN MAXVALUE"
                        + ");"
                        + "INSERT INTO " + s + ".part_single VALUES"
                        + " (1),(2)")
                    .cleanupSql("DROP TABLE IF EXISTS "
                            + s + ".part_single")
                    .expectPrimaryKey("id")
                    .expectRowCount(2)
                    .run();
        }
    }

    @Nested class TableTestsRow extends MariaDbTableCases {
        @Override public boolean useArrow() { return false; }
    }

    @Nested class TableTestsArrow extends MariaDbTableCases {
        @Override public boolean useArrow() { return true; }
    }
}
