package tech.ydb.importer.integration.sources.greenplum;

import java.util.TimeZone;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import tech.ydb.importer.config.SourceType;
import tech.ydb.importer.integration.sources.postgres.AbstractPostgresCompatibleTableCases;
import tech.ydb.importer.integration.sources.postgres.AbstractPostgresCompatibleTypeCases;

/** Greenplum integration tests. */
public class GreenplumYdbImporterTest {

    private static GreenplumTestContainer gpContainer;
    private static TimeZone originalTz;

    @BeforeAll
    static void startGreenplum() {
        originalTz = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        gpContainer = new GreenplumTestContainer();
        gpContainer.start();
    }

    @AfterAll
    static void stopGreenplum() {
        if (gpContainer != null) {
            gpContainer.stop();
            gpContainer = null;
        }
        if (originalTz != null) {
            TimeZone.setDefault(originalTz);
        }
    }

    abstract class GreenplumTypeCases
            extends AbstractPostgresCompatibleTypeCases {
        @Override
        public SourceDb sourceDb() {
            return new SourceDb(gpContainer, SourceType.GREENPLUM, "public");
        }
    }

    @Nested class TypeTestsRow extends GreenplumTypeCases {
        @Override public boolean useArrow() { return false; }
    }

    @Nested class TypeTestsArrow extends GreenplumTypeCases {
        @Override public boolean useArrow() { return true; }
    }

    abstract class GreenplumTableCases
            extends AbstractPostgresCompatibleTableCases {
        @Override
        public SourceDb sourceDb() {
            return new SourceDb(gpContainer, SourceType.GREENPLUM, "public");
        }

        @Test
        @Override
        @Disabled("GP Large Objects (OID) not supported")
        public void partitionedMultiBlobSynthKey() throws Exception {
        }

        /**
         * GP requires every UNIQUE/PK to include the distribution column.
         * pk_over_unique uses a composite PK, uq_fewer_cols includes it explicitly.
         */
        @Test
        @Override
        public void primaryKeySelection() throws Exception {
            String s = schemaName();
            importTogether()
                    
                    .add(tableTest(s, "pk_composite")
                            .setupSql(
                                "CREATE TABLE " + s + ".pk_composite ("
                                + "  region_id INTEGER NOT NULL,"
                                + "  city_id   INTEGER NOT NULL,"
                                + "  name      VARCHAR(100) NOT NULL,"
                                + "  PRIMARY KEY (region_id, city_id)"
                                + ");"
                                + "INSERT INTO " + s + ".pk_composite VALUES"
                                + "  (1, 10, 'Moscow'),"
                                + "  (1, 20, 'SPb'),"
                                + "  (2, 30, 'Berlin')")
                            .cleanupSql(
                                "DROP TABLE IF EXISTS "
                                + s + ".pk_composite CASCADE")
                            .expectPrimaryKey("region_id", "city_id")
                            .expectRowCount(3)
                            .expectRowExists(
                                    "region_id", 1, "city_id", 10,
                                    "name", "Moscow"))
                    .add(tableTest(s, "pk_over_unique")
                            .setupSql(
                                "CREATE TABLE " + s + ".pk_over_unique ("
                                + "  id   BIGINT NOT NULL,"
                                + "  code VARCHAR(20) NOT NULL,"
                                + "  val  INTEGER,"
                                + "  PRIMARY KEY (id, code),"
                                + "  UNIQUE (code)"
                                + ");"
                                + "INSERT INTO " + s + ".pk_over_unique VALUES"
                                + "  (1, 'A', 10),"
                                + "  (2, 'B', 20)")
                            .cleanupSql(
                                "DROP TABLE IF EXISTS "
                                + s + ".pk_over_unique CASCADE")
                            .expectPrimaryKey("id", "code")
                            .expectRowCount(2)
                            .expectRowExists("id", 1L, "code", "A"))
                    .add(tableTest(s, "uq_fewer_cols")
                            .setupSql(
                                "CREATE TABLE " + s + ".uq_fewer_cols ("
                                + "  a BIGINT NOT NULL,"
                                + "  b VARCHAR(10) NOT NULL,"
                                + "  c VARCHAR(10) NOT NULL UNIQUE,"
                                + "  UNIQUE (a, c)"
                                + ");"
                                + "INSERT INTO " + s + ".uq_fewer_cols VALUES"
                                + "  (1, 'x', 'p'),"
                                + "  (2, 'y', 'q')")
                            .cleanupSql(
                                "DROP TABLE IF EXISTS "
                                + s + ".uq_fewer_cols CASCADE")
                            .expectPrimaryKey("c")
                            .expectRowCount(2)
                            .expectRowExists("c", "p"))
                    .add(tableTest(s, "uq_sorted_names")
                            .setupSql(
                                "CREATE TABLE " + s + ".uq_sorted_names ("
                                + "  x BIGINT NOT NULL,"
                                + "  a VARCHAR(10) NOT NULL,"
                                + "  b VARCHAR(10) NOT NULL,"
                                + "  z VARCHAR(10) NOT NULL,"
                                + "  UNIQUE (z, a),"
                                + "  UNIQUE (a, b)"
                                + ");"
                                + "INSERT INTO " + s + ".uq_sorted_names VALUES"
                                + "  (1, 'v1', 'v2', 'v3'),"
                                + "  (2, 'v4', 'v5', 'v6')")
                            .cleanupSql(
                                "DROP TABLE IF EXISTS "
                                + s + ".uq_sorted_names CASCADE")
                            .expectPrimaryKey("a", "b")
                            .expectRowCount(2)
                            .expectRowExists("a", "v1", "b", "v2"))
                    .run();
        }

        @Test
        @Override
        public void skipUnsupportedColumns() throws Exception {
            String s = schemaName();
            tableTest(s, "skip_pg")
                    
                    .withOptions(opts -> opts.setSkipUnknownTypes(true))
                    .setupSql(
                        "CREATE TABLE " + s + ".skip_pg ("
                        + "  id   INTEGER PRIMARY KEY,"
                        + "  name VARCHAR(50) NOT NULL,"
                        + "  uid  UUID,"
                        + "  doc  JSONB,"
                        + "  tags INTEGER[]"
                        + ");"
                        + "INSERT INTO " + s + ".skip_pg"
                        + "  (id, name, uid, doc, tags) VALUES"
                        + "  (1, 'a',"
                        + "   '11111111-1111-1111-1111-111111111111'::uuid,"
                        + "   '{\"k\":1}', ARRAY[1,2]),"
                        + "  (2, 'b',"
                        + "   '22222222-2222-2222-2222-222222222222'::uuid,"
                        + "   '{\"k\":2}', ARRAY[3,4])")
                    .cleanupSql(
                        "DROP TABLE IF EXISTS "
                        + s + ".skip_pg CASCADE")
                    .expectPrimaryKey("id")
                    .expectSkippedColumns("uid", "doc", "tags")
                    .expectRowCount(2)
                    .expectRowExists("id", 1, "name", "a")
                    .expectRowExists("id", 2, "name", "b")
                    .run();
        }

        @ParameterizedTest
        @ValueSource(strings = {"row", "column"})
        public void importFromAoTable(String orientation) throws Exception {
            String s = schemaName();
            String tableName = "gp_ao_" + orientation;
            tableTest(s, tableName)
                    
                    .setupSql(
                        "CREATE TABLE " + s + "." + tableName + " ("
                        + "  id INTEGER NOT NULL PRIMARY KEY,"
                        + "  name VARCHAR(50) NOT NULL,"
                        + "  amount INTEGER NOT NULL"
                        + ") WITH (appendoptimized=true,"
                        + " orientation=" + orientation + ","
                        + " compresstype=zstd,"
                        + " compresslevel=5);"
                        + "INSERT INTO " + s + "." + tableName + " VALUES"
                        + "  (1, 'alice', 100),"
                        + "  (2, 'bob', 200),"
                        + "  (3, 'carol', 300)")
                    .cleanupSql("DROP TABLE IF EXISTS "
                            + s + "." + tableName + " CASCADE")
                    .expectPrimaryKey("id")
                    .expectRowCount(3)
                    .expectRowExists("id", 1, "name", "alice", "amount", 100)
                    .expectRowExists("id", 3, "name", "carol", "amount", 300)
                    .run();
        }
    }

    @Nested class TableTestsRow extends GreenplumTableCases {
        @Override public boolean useArrow() { return false; }
    }

    @Nested class TableTestsArrow extends GreenplumTableCases {
        @Override public boolean useArrow() { return true; }
    }
}
