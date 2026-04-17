package tech.ydb.importer.integration.sources.mariadb.columnstore;

import java.util.TimeZone;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;

import tech.ydb.importer.config.SourceType;
import tech.ydb.importer.integration.sources.mysql.AbstractMySqlCompatibleTableCases;
import tech.ydb.importer.integration.sources.mysql.AbstractMySqlCompatibleTypeCases;
import tech.ydb.table.values.PrimitiveType;

/** MariaDB ColumnStore integration tests. */
public class MariaDbColumnStoreYdbImporterTest {

    private static final String DB_NAME = ColumnStoreContainer.DEFAULT_DB_NAME;

    private static ColumnStoreContainer csContainer;
    private static JdbcDatabaseContainer<?> jdbcContainer;
    private static TimeZone originalTz;

    @BeforeAll
    static void startColumnStore() throws Exception {
        originalTz = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

        csContainer = new ColumnStoreContainer();
        csContainer.start();
        jdbcContainer = csContainer.getJdbcContainer();
    }

    @AfterAll
    static void stopColumnStore() {
        if (csContainer != null) {
            csContainer.stop();
            csContainer = null;
        }
        jdbcContainer = null;
        if (originalTz != null) {
            TimeZone.setDefault(originalTz);
        }
    }

    abstract class CsTypeCases
            extends AbstractMySqlCompatibleTypeCases {
        @Override
        public SourceDb sourceDb() {
            return new SourceDb(jdbcContainer, SourceType.MARIADB, DB_NAME);
        }

        @Override
        protected String createTableSuffix() {
            return " ENGINE=Columnstore";
        }

        // ColumnStore BOOLEAN reports as TINYINT, not BIT.
        @Test
        @Override
        public void booleanMaps() throws Exception {
            typeTest().withArrow(useArrow())
                    .column("TINYINT NOT NULL", PrimitiveType.Int32)
                        .value("1", 1)
                        .value("0", 0)
                    .execute();
        }

        // ColumnStore integer ranges slightly smaller than standard.
        @Test
        @Override
        public void tinyintMapsToInt32() throws Exception {
            typeTest().withArrow(useArrow())
                    .column("TINYINT NOT NULL", PrimitiveType.Int32)
                        .value("126", 126)
                        .value("-126", -126)
                    .execute();
        }

        @Test
        @Override
        public void int32Boundaries() throws Exception {
            typeTest().withArrow(useArrow())
                    .column("INT NOT NULL", PrimitiveType.Int32)
                        .value("0", 0)
                        .value("-1", -1)
                        .value("2147483646", 2147483646)
                    .column("INT", PrimitiveType.Int32.makeOptional())
                        .value("42", 42)
                        .value("NULL", null)
                    .execute();
        }

        @Test
        @Override
        public void int64Boundaries() throws Exception {
            typeTest().withArrow(useArrow())
                    .column("BIGINT NOT NULL", PrimitiveType.Int64)
                        .value("0", 0L)
                        .value("999999999999999", 999999999999999L)
                    .execute();
        }

        @Test
        @Override
        public void smallintMapsToInt32() throws Exception {
            typeTest().withArrow(useArrow())
                    .column("SMALLINT NOT NULL", PrimitiveType.Int32)
                        .value("32766", 32766)
                        .value("-32766", -32766)
                    .execute();
        }

        @Test
        @Override
        public void mediumintMapsToInt32() throws Exception {
            typeTest().withArrow(useArrow())
                    .column("MEDIUMINT NOT NULL", PrimitiveType.Int32)
                        .value("8388606", 8388606)
                        .value("-8388606", -8388606)
                    .execute();
        }

        @Test
        @Override
        @Disabled("ColumnStore does not support JSON/ENUM")
        public void jsonAndEnumMapToText() throws Exception {
        }

        @Test
        @Override
        @Disabled("ColumnStore does not support VARBINARY")
        public void binaryAndVarbinary() throws Exception {
        }

        // ColumnStore treats zero-length string as NULL.
        @Test
        @Override
        public void stringTypesMapToText() throws Exception {
            typeTest().withArrow(useArrow())
                    .column("VARCHAR(100) NOT NULL", PrimitiveType.Text)
                        .value("'hello'", "hello")
                        .value("'кириллица'", "кириллица")
                    .column("TEXT NOT NULL", PrimitiveType.Text)
                        .value("'hello'", "hello")
                        .value("'text value'", "text value")
                    .column("VARCHAR(100)",
                            PrimitiveType.Text.makeOptional())
                        .value("'hello'", "hello")
                        .value("''", null) // empty string -> NULL
                    .execute();
        }

        @Test
        @Override
        @Disabled("ColumnStore DECIMAL precision limited to 18")
        public void highPrecisionDecimalMapsToDecimal() throws Exception {
        }
    }

    @Nested class TypeTestsRow extends CsTypeCases {
        @Override protected boolean useArrow() { return false; }
    }

    @Nested class TypeTestsArrow extends CsTypeCases {
        @Override protected boolean useArrow() { return true; }
    }

    abstract class CsTableCases
            extends AbstractMySqlCompatibleTableCases {
        @Override
        public SourceDb sourceDb() {
            return new SourceDb(jdbcContainer, SourceType.MARIADB, DB_NAME);
        }

        @Override
        protected String engine() {
            return "Columnstore";
        }

        // ColumnStore cannot have PRIMARY KEY in DDL.
        @Test
        @Override
        public void customQueryTextImport() throws Exception {
            String s = schemaName();
            tableTest(s, "query_src")
                    .withArrow(useArrow())
                    .setupSql(
                        "CREATE TABLE " + s + ".query_src ("
                        + "id INT NOT NULL,"
                        + "val VARCHAR(50) NOT NULL"
                        + ") ENGINE=Columnstore;"
                        + "INSERT INTO " + s + ".query_src VALUES"
                        + " (1,'a'),(2,'b'),(3,'c'),(4,'d')")
                    .cleanupSql("DROP TABLE IF EXISTS "
                            + s + ".query_src")
                    .queryText("SELECT id, val FROM " + s
                            + ".query_src WHERE id <= 2")
                    .expectSyntheticKey()
                    .expectRowCount(2)
                    .expectRowExists("id", 1, "val", "a")
                    .expectRowExists("id", 2, "val", "b")
                    .run();
        }

        @Test
        @Override
        public void emptyTableCreatesSchema() throws Exception {
            String s = schemaName();
            tableTest(s, "empty_tbl")
                    .withArrow(useArrow())
                    .setupSql(
                        "CREATE TABLE " + s + ".empty_tbl ("
                        + "id INT NOT NULL,"
                        + "name VARCHAR(100) NOT NULL"
                        + ") ENGINE=Columnstore")
                    .cleanupSql("DROP TABLE IF EXISTS "
                            + s + ".empty_tbl")
                    .expectSyntheticKey()
                    .expectRowCount(0)
                    .run();
        }

        /*
         * ColumnStore ignores PRIMARY KEY / UNIQUE,
         * all tables fall back to synthetic key.
         */
        @Test
        @Override
        public void primaryKeySelection() throws Exception {
            String s = schemaName();
            String e = engine();
            importTogether()
                    .withArrow(useArrow())
                    .add(tableTest(s, "pk_composite")
                            .setupSql(
                                "CREATE TABLE " + s + ".pk_composite ("
                                + "region_id INT NOT NULL,"
                                + "city_id INT NOT NULL,"
                                + "name VARCHAR(100) NOT NULL"
                                + ") ENGINE=" + e + ";"
                                + "INSERT INTO " + s
                                + ".pk_composite VALUES"
                                + " (1,10,'Moscow'),(1,20,'SPb'),"
                                + "(2,30,'Berlin')")
                            .cleanupSql("DROP TABLE IF EXISTS "
                                    + s + ".pk_composite")
                            .expectSyntheticKey()
                            .expectRowCount(3)
                            .expectRowExists(
                                    "region_id", 1, "city_id", 10,
                                    "name", "Moscow"))
                    .add(tableTest(s, "pk_over_unique")
                            .setupSql(
                                "CREATE TABLE " + s + ".pk_over_unique ("
                                + "id BIGINT NOT NULL,"
                                + "code VARCHAR(20) NOT NULL,"
                                + "val INT"
                                + ") ENGINE=" + e + ";"
                                + "INSERT INTO " + s
                                + ".pk_over_unique"
                                + " VALUES (1,'A',10),(2,'B',20)")
                            .cleanupSql("DROP TABLE IF EXISTS "
                                    + s + ".pk_over_unique")
                            .expectSyntheticKey()
                            .expectRowCount(2)
                            .expectRowExists("id", 1L, "code", "A"))
                    .add(tableTest(s, "uq_as_key")
                            .setupSql(
                                "CREATE TABLE " + s + ".uq_as_key ("
                                + "a BIGINT NOT NULL,"
                                + "b VARCHAR(10) NOT NULL"
                                + ") ENGINE=" + e + ";"
                                + "INSERT INTO " + s + ".uq_as_key"
                                + " VALUES (1,'x'),(2,'y')")
                            .cleanupSql("DROP TABLE IF EXISTS "
                                    + s + ".uq_as_key")
                            .expectSyntheticKey()
                            .expectRowCount(2))
                    .run();
        }

        @Test
        @Override
        @Disabled("ColumnStore engine does not support PARTITION BY")
        public void partitionedImport() throws Exception {
        }

        // ColumnStore engine does not support PARTITION BY or PRIMARY KEY in DDL.
        @Test
        @Override
        public void partitionedMultiBlobSynthKey() throws Exception {
            String s = schemaName();
            String e = engine();

            byte[] thumb1 = "thumb-1".getBytes();
            byte[] thumb2 = "thumb-2".getBytes();
            byte[] thumb3 = "thumb-3".getBytes();
            byte[] thumb4 = "thumb-4".getBytes();

            byte[] full1 = filled(500_000, (byte) 0x11);
            byte[] full2 = filled(500_000, (byte) 0x22);
            byte[] full3 = filled(500_000, (byte) 0x33);
            byte[] full4 = filled(500_000, (byte) 0x44);

            tableTest(s, "mega_blob")
                    .withArrow(useArrow())
                    .setupSql(
                        "CREATE TABLE " + s + ".mega_blob ("
                        + "  label     VARCHAR(30),"
                        + "  thumbnail MEDIUMBLOB,"
                        + "  fullsize  MEDIUMBLOB"
                        + ") ENGINE=" + e + ";"
                        + "INSERT INTO " + s + ".mega_blob VALUES"
                        + "('book-1'," + smallBlob(thumb1)
                        + "," + bigBlob("11", 500_000) + "),"
                        + "('book-2'," + smallBlob(thumb2)
                        + "," + bigBlob("22", 500_000) + "),"
                        + "('null-row',NULL,NULL),"
                        + "('book-3'," + smallBlob(thumb3)
                        + "," + bigBlob("33", 500_000) + "),"
                        + "('book-4'," + smallBlob(thumb4)
                        + "," + bigBlob("44", 500_000) + ")")
                    .cleanupSql("DROP TABLE IF EXISTS "
                            + s + ".mega_blob")
                    .expectSyntheticKey()
                    .expectRowCount(5)
                    .expectBlobColumn("thumbnail")
                    .expectBlobColumn("fullsize")
                    .expectBlobBytes("thumbnail", "label", "book-1", thumb1)
                    .expectBlobBytes("thumbnail", "label", "book-2", thumb2)
                    .expectBlobBytes("thumbnail", "label", "null-row", null)
                    .expectBlobBytes("thumbnail", "label", "book-3", thumb3)
                    .expectBlobBytes("thumbnail", "label", "book-4", thumb4)
                    .expectBlobBytes("fullsize", "label", "book-1", full1)
                    .expectBlobBytes("fullsize", "label", "book-2", full2)
                    .expectBlobBytes("fullsize", "label", "null-row", null)
                    .expectBlobBytes("fullsize", "label", "book-3", full3)
                    .expectBlobBytes("fullsize", "label", "book-4", full4)
                    .run();

            tableTest(s, "small_blob")
                    .withArrow(useArrow())
                    .setupSql(
                        "CREATE TABLE " + s + ".small_blob ("
                        + "  id   INT NOT NULL,"
                        + "  name VARCHAR(50) NOT NULL,"
                        + "  data MEDIUMBLOB"
                        + ") ENGINE=" + e + ";"
                        + "INSERT INTO " + s + ".small_blob VALUES"
                        + "  (1, 'hello',     " + smallBlob("Hello".getBytes()) + "),"
                        + "  (2, 'null_blob', NULL),"
                        + "  (3, 'world',     " + smallBlob("World".getBytes()) + ")")
                    .cleanupSql("DROP TABLE IF EXISTS "
                            + s + ".small_blob")
                    .expectSyntheticKey()
                    .expectRowCount(3)
                    .expectBlobColumn("data")
                    .expectBlobBytes("data", "id", 1, "Hello".getBytes())
                    .expectBlobBytes("data", "id", 3, "World".getBytes())
                    .expectBlobBytes("data", "id", 2, null)
                    .run();
        }
    }

    @Nested class TableTestsRow extends CsTableCases {
        @Override protected boolean useArrow() { return false; }
    }

    @Nested class TableTestsArrow extends CsTableCases {
        @Override protected boolean useArrow() { return true; }
    }
}
