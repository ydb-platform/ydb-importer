package tech.ydb.importer.integration.sources.hana;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.TimeZone;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import tech.ydb.importer.config.SourceType;
import tech.ydb.importer.config.TableOptions.DateConv;
import tech.ydb.importer.integration.tabletest.AbstractYdbImporterTableTest;
import tech.ydb.importer.integration.typetest.AbstractYdbImporterTypeTest;
import tech.ydb.table.values.DecimalType;
import tech.ydb.table.values.PrimitiveType;

/** SAP HANA integration tests. */
public class HanaYdbImporterTest {

    private static HanaTestContainer hanaContainer;
    private static TimeZone originalTz;

    @BeforeAll
    static void startHana() {
        originalTz = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        hanaContainer = new HanaTestContainer();
        hanaContainer.start();
    }

    @AfterAll
    static void stopHana() {
        if (hanaContainer != null) {
            hanaContainer.stop();
            hanaContainer = null;
        }
        if (originalTz != null) {
            TimeZone.setDefault(originalTz);
        }
    }

    abstract class HanaTypeCases extends AbstractYdbImporterTypeTest {

        protected abstract boolean useArrow();

        @Override
        public SourceDb sourceDb() {
            return new SourceDb(hanaContainer, SourceType.HANA, "IMPORT_TEST");
        }

        /*
         * HANA folds unquoted identifiers to UPPERCASE. The importer sends
         * quoted lowercase names via HanaTableLister.safeId, so we must also
         * create tables with quoted identifiers to preserve case.
         */
        @Override
        protected tech.ydb.importer.integration.typetest.TypeTestBuilder typeTest() {
            return super.typeTest().withIdentifierQuote("\"");
        }

        @Test
        public void tinyintMapsToInt32() throws Exception {
            // HANA TINYINT is UNSIGNED 0..255 (unusual among databases).
            typeTest().withArrow(useArrow())
                    .column("TINYINT NOT NULL", PrimitiveType.Int32)
                        .value("0", 0)
                        .value("255", 255)
                    .column("TINYINT", PrimitiveType.Int32.makeOptional())
                        .value("42", 42)
                        .value("NULL", null)
                    .execute();
        }

        @Test
        public void smallintMapsToInt32() throws Exception {
            typeTest().withArrow(useArrow())
                    .column("SMALLINT NOT NULL", PrimitiveType.Int32)
                        .value("0", 0)
                        .value("32767", 32767)
                        .value("-32768", -32768)
                    .execute();
        }

        @Test
        public void integerBoundaries() throws Exception {
            typeTest().withArrow(useArrow())
                    .column("INTEGER NOT NULL", PrimitiveType.Int32)
                        .value("0", 0)
                        .value("2147483647", Integer.MAX_VALUE)
                        .value("-2147483648", Integer.MIN_VALUE)
                    .column("INTEGER", PrimitiveType.Int32.makeOptional())
                        .value("42", 42)
                        .value("NULL", null)
                    .execute();
        }

        @Test
        public void bigintBoundaries() throws Exception {
            typeTest().withArrow(useArrow())
                    .column("BIGINT NOT NULL", PrimitiveType.Int64)
                        .value("0", 0L)
                        .value("9223372036854775807", Long.MAX_VALUE)
                        .value("-9223372036854775808", Long.MIN_VALUE)
                    .execute();
        }

        @Test
        public void doublePrecision() throws Exception {
            typeTest().withArrow(useArrow())
                    .column("DOUBLE NOT NULL", PrimitiveType.Double)
                        .value("1.5", 1.5d)
                        .value("1e100", 1e100d)
                    .column("DOUBLE", PrimitiveType.Double.makeOptional())
                        .value("9.99", 9.99d)
                        .value("NULL", null)
                    .execute();
        }

        @Test
        public void realMapsToFloat() throws Exception {
            // HANA REAL is 4-byte IEEE 754 single - exactly YDB Float.
            typeTest().withArrow(useArrow())
                    .column("REAL NOT NULL", PrimitiveType.Float)
                        .value("1.5", 1.5f)
                        .value("-0.25", -0.25f)
                    .column("REAL", PrimitiveType.Float.makeOptional())
                        .value("3.14", 3.14f)
                        .value("NULL", null)
                    .execute();
        }

        @Test
        public void decimalWithScale() throws Exception {
            typeTest().withArrow(useArrow())
                    .column("DECIMAL(10, 4) NOT NULL",
                            DecimalType.of(10, 4))
                        .value("123.4567", new BigDecimal("123.4567"))
                        .value("-999999.9999",
                                new BigDecimal("-999999.9999"))
                    .column("DECIMAL(10, 2)",
                            DecimalType.of(10, 2).makeOptional())
                        .value("99.99", new BigDecimal("99.99"))
                        .value("NULL", null)
                    .execute();
        }

        @Test
        public void decimalZeroScaleToInt32() throws Exception {
            typeTest().withArrow(useArrow())
                    .column("DECIMAL(5, 0) NOT NULL", PrimitiveType.Int32)
                        .value("12345", 12345)
                        .value("-99999", -99999)
                    .execute();
        }

        @Test
        public void decimalHighPrecisionToDecimal() throws Exception {
            typeTest().withArrow(useArrow())
                    .column("DECIMAL(25, 0) NOT NULL",
                            DecimalType.of(35, 0))
                        .value("1234567890123456789012345",
                                new BigDecimal(
                                        "1234567890123456789012345"))
                    .execute();
        }

        @Test
        public void smallDecimalMaps() throws Exception {
            /*
             * SMALLDECIMAL (HANA 16-byte IEEE) reports as DECIMAL(16, 0),
             * so scale=0 + precision in [10, 20) policy picks Int64.
             */
            typeTest().withArrow(useArrow())
                    .column("SMALLDECIMAL NOT NULL", PrimitiveType.Int64)
                        .value("12345", 12345L)
                        .value("-67890", -67890L)
                    .execute();
        }

        @Test
        public void nvarcharUnicode() throws Exception {
            typeTest().withArrow(useArrow())
                    .column("NVARCHAR(100) NOT NULL", PrimitiveType.Text)
                        .value("'hello'", "hello")
                        .value("''", "")
                        .value("'кириллица'", "кириллица")
                        .value("'emoji \uD83D\uDE00'", "emoji \uD83D\uDE00")
                    .column("NVARCHAR(100)",
                            PrimitiveType.Text.makeOptional())
                        .value("'hello'", "hello")
                        .value("NULL", null)
                    .execute();
        }

        @Test
        public void varcharAsciiMaps() throws Exception {
            // HANA 2.x VARCHAR is ASCII-only. We just assert round-trip.
            typeTest().withArrow(useArrow())
                    .column("VARCHAR(100) NOT NULL", PrimitiveType.Text)
                        .value("'hello'", "hello")
                        .value("''", "")
                    .execute();
        }

        @Test
        public void alphanumMaps() throws Exception {
            typeTest().withArrow(useArrow())
                    .column("ALPHANUM(20) NOT NULL", PrimitiveType.Text)
                        .value("'ABC123'", "ABC123")
                        .value("'XYZ'", "XYZ")
                    .execute();
        }

        @Test
        public void varbinaryMapsToBytes() throws Exception {
            typeTest().withArrow(useArrow())
                    .column("VARBINARY(100) NOT NULL", PrimitiveType.Bytes)
                        .value("x'DEADBEEF'",
                                new byte[]{(byte) 0xDE, (byte) 0xAD,
                                           (byte) 0xBE, (byte) 0xEF})
                        .value("x'00'", new byte[]{(byte) 0x00})
                    .execute();
        }

        @Test
        public void booleanMaps() throws Exception {
            typeTest().withArrow(useArrow())
                    .column("BOOLEAN NOT NULL", PrimitiveType.Bool)
                        .value("TRUE", true)
                        .value("FALSE", false)
                    .column("BOOLEAN", PrimitiveType.Bool.makeOptional())
                        .value("TRUE", true)
                        .value("NULL", null)
                    .execute();
        }

        @Test
        public void dateMaps() throws Exception {
            typeTest().withArrow(useArrow())
                    .column("DATE NOT NULL", PrimitiveType.Date32)
                        .value("'1970-01-01'",
                                LocalDate.of(1970, 1, 1))
                        .value("'2024-01-15'",
                                LocalDate.of(2024, 1, 15))
                    .column("DATE", PrimitiveType.Date32.makeOptional())
                        .value("'2024-01-15'",
                                LocalDate.of(2024, 1, 15))
                        .value("NULL", null)
                    .execute();
        }

        @Test
        public void dateAsInt() throws Exception {
            typeTest().withArrow(useArrow())
                    .withOptions(
                            opts -> opts.setDateConv(DateConv.INT))
                    .column("DATE NOT NULL", PrimitiveType.Int32)
                        .value("'1970-01-01'", 19700101)
                        .value("'2024-01-15'", 20240115)
                    .execute();
        }

        @Test
        public void dateAsText() throws Exception {
            typeTest().withArrow(useArrow())
                    .withOptions(
                            opts -> opts.setDateConv(DateConv.STR))
                    .column("DATE NOT NULL", PrimitiveType.Text)
                        .value("'1970-01-01'", "1970/01/01")
                        .value("'2024-01-15'", "2024/01/15")
                    .execute();
        }

        @Test
        public void timeMapsToInt32() throws Exception {
            typeTest().withArrow(useArrow())
                    .column("TIME NOT NULL", PrimitiveType.Int32)
                        .value("'00:00:00'", 0)
                        .value("'10:30:45'", 37845)
                        .value("'23:59:59'", 86399)
                    .execute();
        }

        @Test
        public void seconddateMaps() throws Exception {
            /*
             * SECONDDATE has second precision. Driver reports scale=0.
             * YDB SDK returns Datetime64 values as LocalDateTime.
             */
            typeTest().withArrow(useArrow())
                    .column("SECONDDATE NOT NULL",
                            PrimitiveType.Datetime64)
                        .value("'2024-01-15 10:30:45'",
                                java.time.LocalDateTime.of(
                                        2024, 1, 15, 10, 30, 45))
                    .execute();
        }

        @Test
        public void timestampMaps() throws Exception {
            /*
             * HANA TIMESTAMP has 100ns precision (scale=7). YDB Timestamp64
             * is microseconds, so values round down at the us boundary.
             */
            typeTest().withArrow(useArrow())
                    .column("TIMESTAMP NOT NULL", PrimitiveType.Timestamp64)
                        .value("'2024-01-15 10:30:45'",
                                Instant.parse(
                                        "2024-01-15T10:30:45Z"))
                        .value("'2024-01-15 10:30:45.123456'",
                                Instant.parse(
                                    "2024-01-15T10:30:45.123456Z"))
                    .execute();
        }
    }

    @Nested class TypeTestsRow extends HanaTypeCases {
        @Override protected boolean useArrow() { return false; }
    }

    @Nested class TypeTestsArrow extends HanaTypeCases {
        @Override protected boolean useArrow() { return true; }
    }

    abstract class HanaTableCases extends AbstractYdbImporterTableTest {

        /*
         * 512 KB spans multiple 64 KB blob-table chunks. HANA has no SQL-level
         * BLOB concatenation (no ||, no CONCAT, no BLOB_AGG), so large values
         * are assembled in Java and pushed via PreparedStatement.setBytes.
         */
        private static final int BIG_BLOB_SIZE = 512_000;

        protected abstract boolean useArrow();

        @Override
        public SourceDb sourceDb() {
            return new SourceDb(hanaContainer, SourceType.HANA, "IMPORT_TEST");
        }

        @Override
        protected String smallBlob(byte[] data) {
            return "TO_BLOB(HEXTOBIN('" + hex(data) + "'))";
        }

        // Same as base, but identifiers are UPPERCASE.
        @Test
        public void primaryKeySelection() throws Exception {
            String s = schemaName();
            importTogether()
                    .withArrow(useArrow())
                    .add(tableTest(s, "PK_COMPOSITE")
                            .setupSql(
                                "CREATE TABLE " + s + ".PK_COMPOSITE ("
                                + "REGION_ID INTEGER NOT NULL,"
                                + "CITY_ID   INTEGER NOT NULL,"
                                + "NAME      NVARCHAR(100) NOT NULL,"
                                + "PRIMARY KEY (REGION_ID, CITY_ID));"
                                + "INSERT INTO " + s + ".PK_COMPOSITE VALUES"
                                + "  (1, 10, 'Moscow');"
                                + "INSERT INTO " + s + ".PK_COMPOSITE VALUES"
                                + "  (1, 20, 'SPb');"
                                + "INSERT INTO " + s + ".PK_COMPOSITE VALUES"
                                + "  (2, 30, 'Berlin')")
                            .cleanupSql(
                                "DROP TABLE " + s + ".PK_COMPOSITE CASCADE")
                            .expectPrimaryKey("REGION_ID", "CITY_ID")
                            .expectRowCount(3)
                            .expectRowExists(
                                    "REGION_ID", 1, "CITY_ID", 10,
                                    "NAME", "Moscow"))
                    .add(tableTest(s, "PK_OVER_UNIQUE")
                            .setupSql(
                                "CREATE TABLE " + s + ".PK_OVER_UNIQUE ("
                                + "ID   BIGINT NOT NULL PRIMARY KEY,"
                                + "CODE NVARCHAR(20) NOT NULL,"
                                + "VAL  INTEGER,"
                                + "CONSTRAINT UQ_CODE UNIQUE (CODE));"
                                + "INSERT INTO " + s + ".PK_OVER_UNIQUE VALUES"
                                + "  (1, 'A', 10);"
                                + "INSERT INTO " + s + ".PK_OVER_UNIQUE VALUES"
                                + "  (2, 'B', 20)")
                            .cleanupSql(
                                "DROP TABLE " + s + ".PK_OVER_UNIQUE CASCADE")
                            .expectPrimaryKey("ID")
                            .expectRowCount(2)
                            .expectRowExists("ID", 1L, "CODE", "A"))
                    .add(tableTest(s, "UQ_FEWER_COLS")
                            .setupSql(
                                "CREATE TABLE " + s + ".UQ_FEWER_COLS ("
                                + "A BIGINT NOT NULL,"
                                + "B NVARCHAR(10) NOT NULL,"
                                + "C NVARCHAR(10) NOT NULL,"
                                + "CONSTRAINT UQ_C UNIQUE (C),"
                                + "CONSTRAINT UQ_AB UNIQUE (A, B));"
                                + "INSERT INTO " + s + ".UQ_FEWER_COLS VALUES"
                                + "  (1, 'x', 'p');"
                                + "INSERT INTO " + s + ".UQ_FEWER_COLS VALUES"
                                + "  (2, 'y', 'q')")
                            .cleanupSql(
                                "DROP TABLE " + s + ".UQ_FEWER_COLS CASCADE")
                            .expectPrimaryKey("C")
                            .expectRowCount(2)
                            .expectRowExists("C", "p"))
                    /*
                     * HANA constraint names are unique per schema. Prefix UQ_S_
                     * to avoid clash with UQ_AB from UQ_FEWER_COLS.
                     */
                    .add(tableTest(s, "UQ_SORTED_NAMES")
                            .setupSql(
                                "CREATE TABLE " + s + ".UQ_SORTED_NAMES ("
                                + "X BIGINT NOT NULL,"
                                + "A NVARCHAR(10) NOT NULL,"
                                + "B NVARCHAR(10) NOT NULL,"
                                + "Z NVARCHAR(10) NOT NULL,"
                                + "CONSTRAINT UQ_S_ZA UNIQUE (Z, A),"
                                + "CONSTRAINT UQ_S_AB UNIQUE (A, B));"
                                + "INSERT INTO " + s + ".UQ_SORTED_NAMES VALUES"
                                + "  (1, 'v1', 'v2', 'v3');"
                                + "INSERT INTO " + s + ".UQ_SORTED_NAMES VALUES"
                                + "  (2, 'v4', 'v5', 'v6')")
                            .cleanupSql(
                                "DROP TABLE " + s + ".UQ_SORTED_NAMES CASCADE")
                            .expectPrimaryKey("A", "B")
                            .expectRowCount(2)
                            .expectRowExists("A", "v1", "B", "v2"))
                    .run();
        }

        @Test
        public void partitionedImport() throws Exception {
            String s = schemaName();
            tableTest(s, "PART_SALES")
                    .withArrow(useArrow())
                    .setupSql(
                        "CREATE COLUMN TABLE " + s + ".PART_SALES ("
                        + "SALE_ID   INTEGER NOT NULL,"
                        + "REGION_ID INTEGER NOT NULL,"
                        + "AMOUNT    INTEGER NOT NULL,"
                        + "PRIMARY KEY (SALE_ID, REGION_ID)"
                        + ") PARTITION BY HASH (SALE_ID) PARTITIONS 4;"
                        + "INSERT INTO " + s + ".PART_SALES VALUES (1, 10, 100);"
                        + "INSERT INTO " + s + ".PART_SALES VALUES (2, 20, 200);"
                        + "INSERT INTO " + s + ".PART_SALES VALUES (3, 30, 300);"
                        + "INSERT INTO " + s + ".PART_SALES VALUES (4, 40, 400);"
                        + "INSERT INTO " + s + ".PART_SALES VALUES (5, 50, 500);"
                        + "INSERT INTO " + s + ".PART_SALES VALUES (6, 60, 600);"
                        + "INSERT INTO " + s + ".PART_SALES VALUES (7, 70, 700);"
                        + "INSERT INTO " + s + ".PART_SALES VALUES (8, 80, 800)")
                    .cleanupSql(
                        "DROP TABLE " + s + ".PART_SALES CASCADE")
                    .expectPrimaryKey("SALE_ID", "REGION_ID")
                    .expectPartitionsMin(4)
                    .expectRowCount(8)
                    .expectRowExists("SALE_ID", 1, "AMOUNT", 100)
                    .expectRowExists("SALE_ID", 8, "AMOUNT", 800)
                    .run();
        }

        @Test
        public void partitionedMultiBlobSynthKey() throws Exception {
            String s = schemaName();

            byte[] thumbEU1 = "thumb-eu-1".getBytes();
            byte[] thumbEU2 = "thumb-eu-2".getBytes();
            byte[] thumbUS1 = "thumb-us-1".getBytes();
            byte[] thumbUS2 = "thumb-us-2".getBytes();

            byte[] fullEU1 = filled(BIG_BLOB_SIZE, (byte) 0x11);
            byte[] fullEU2 = filled(BIG_BLOB_SIZE, (byte) 0x22);
            byte[] fullUS1 = filled(BIG_BLOB_SIZE, (byte) 0x33);
            byte[] fullUS2 = filled(BIG_BLOB_SIZE, (byte) 0x44);

            String insertSql = "INSERT INTO " + s + ".MEGA_BLOB"
                    + " VALUES (?, ?, ?, ?)";
            tableTest(s, "MEGA_BLOB")
                    .withArrow(useArrow())
                    .setupSql(
                        "CREATE COLUMN TABLE " + s + ".MEGA_BLOB ("
                        + "REGION    NVARCHAR(10) NOT NULL,"
                        + "LABEL     NVARCHAR(30),"
                        + "THUMBNAIL BLOB,"
                        + "FULLSIZE  BLOB"
                        + ") PARTITION BY RANGE (REGION) ("
                        + "  PARTITION VALUE = 'EU',"
                        + "  PARTITION VALUE = 'US')")
                    .insertRow(insertSql, "EU", "eu-book-1", thumbEU1, fullEU1)
                    .insertRow(insertSql, "EU", "eu-book-2", thumbEU2, fullEU2)
                    .insertRow(insertSql, "EU", "eu-null",   null,     null)
                    .insertRow(insertSql, "US", "us-book-1", thumbUS1, fullUS1)
                    .insertRow(insertSql, "US", "us-book-2", thumbUS2, fullUS2)
                    .cleanupSql("DROP TABLE " + s + ".MEGA_BLOB CASCADE")
                    .expectSyntheticKey()
                    .expectPartitionsMin(2)
                    .expectRowCount(5)
                    .expectBlobColumn("THUMBNAIL")
                    .expectBlobColumn("FULLSIZE")
                    .expectBlobBytes("THUMBNAIL", "LABEL", "eu-book-1", thumbEU1)
                    .expectBlobBytes("THUMBNAIL", "LABEL", "eu-book-2", thumbEU2)
                    .expectBlobBytes("THUMBNAIL", "LABEL", "eu-null", null)
                    .expectBlobBytes("THUMBNAIL", "LABEL", "us-book-1", thumbUS1)
                    .expectBlobBytes("THUMBNAIL", "LABEL", "us-book-2", thumbUS2)
                    .expectBlobBytes("FULLSIZE", "LABEL", "eu-book-1", fullEU1)
                    .expectBlobBytes("FULLSIZE", "LABEL", "eu-book-2", fullEU2)
                    .expectBlobBytes("FULLSIZE", "LABEL", "eu-null", null)
                    .expectBlobBytes("FULLSIZE", "LABEL", "us-book-1", fullUS1)
                    .expectBlobBytes("FULLSIZE", "LABEL", "us-book-2", fullUS2)
                    .run();

            tableTest(s, "SMALL_BLOB")
                    .withArrow(useArrow())
                    .setupSql(
                        "CREATE COLUMN TABLE " + s + ".SMALL_BLOB ("
                        + "ID   INTEGER NOT NULL PRIMARY KEY,"
                        + "NAME NVARCHAR(50) NOT NULL,"
                        + "DATA BLOB);"
                        + "INSERT INTO " + s + ".SMALL_BLOB VALUES"
                        + "  (1, 'hello',     " + smallBlob("Hello".getBytes()) + ");"
                        + "INSERT INTO " + s + ".SMALL_BLOB VALUES"
                        + "  (2, 'null_blob', NULL);"
                        + "INSERT INTO " + s + ".SMALL_BLOB VALUES"
                        + "  (3, 'world',     " + smallBlob("World".getBytes()) + ")")
                    .cleanupSql("DROP TABLE " + s + ".SMALL_BLOB CASCADE")
                    .expectPrimaryKey("ID")
                    .expectRowCount(3)
                    .expectBlobColumn("DATA")
                    .expectBlobBytes("DATA", "ID", 1, "Hello".getBytes())
                    .expectBlobBytes("DATA", "ID", 3, "World".getBytes())
                    .expectBlobBytes("DATA", "ID", 2, null)
                    .run();
        }

        @Test
        public void emptyTableCreatesSchema() throws Exception {
            String s = schemaName();
            tableTest(s, "EMPTY_TBL")
                    .withArrow(useArrow())
                    .setupSql(
                        "CREATE TABLE " + s + ".EMPTY_TBL ("
                        + "ID   INTEGER NOT NULL,"
                        + "NAME NVARCHAR(50) NOT NULL,"
                        + "PRIMARY KEY (ID))")
                    .cleanupSql("DROP TABLE " + s + ".EMPTY_TBL CASCADE")
                    .expectPrimaryKey("ID")
                    .expectRowCount(0)
                    .run();
        }

        @Test
        public void customQueryTextImport() throws Exception {
            String s = schemaName();
            tableTest(s, "QUERY_SRC")
                    .withArrow(useArrow())
                    .setupSql(
                        "CREATE TABLE " + s + ".QUERY_SRC ("
                        + "ID  INTEGER NOT NULL,"
                        + "VAL NVARCHAR(50) NOT NULL,"
                        + "PRIMARY KEY (ID));"
                        + "INSERT INTO " + s + ".QUERY_SRC VALUES (1, 'a');"
                        + "INSERT INTO " + s + ".QUERY_SRC VALUES (2, 'b');"
                        + "INSERT INTO " + s + ".QUERY_SRC VALUES (3, 'c');"
                        + "INSERT INTO " + s + ".QUERY_SRC VALUES (4, 'd')")
                    .cleanupSql("DROP TABLE " + s + ".QUERY_SRC CASCADE")
                    .queryText("SELECT ID, VAL FROM " + s + ".QUERY_SRC"
                            + " WHERE ID <= 2")
                    .expectSyntheticKey()
                    .expectRowCount(2)
                    .expectRowExists("ID", 1, "VAL", "a")
                    .expectRowExists("ID", 2, "VAL", "b")
                    .run();
        }

        @Test
        public void syntheticKeyDistinguishesRows() throws Exception {
            // HANA column-store has no PK. Importer assigns synthetic key.
            String s = schemaName();
            tableTest(s, "SYNTH_TBL")
                    .withArrow(useArrow())
                    .setupSql(
                        "CREATE COLUMN TABLE " + s + ".SYNTH_TBL ("
                        + "SEQ  INTEGER NOT NULL,"
                        + "NAME NVARCHAR(50) NOT NULL);"
                        + "INSERT INTO " + s + ".SYNTH_TBL VALUES (1, 'same');"
                        + "INSERT INTO " + s + ".SYNTH_TBL VALUES (2, 'same');"
                        + "INSERT INTO " + s + ".SYNTH_TBL VALUES (3, 'same')")
                    .cleanupSql("DROP TABLE " + s + ".SYNTH_TBL CASCADE")
                    .expectSyntheticKey()
                    .expectRowCount(3)
                    .expectRowExists("SEQ", 1, "NAME", "same")
                    .expectRowExists("SEQ", 2, "NAME", "same")
                    .expectRowExists("SEQ", 3, "NAME", "same")
                    .run();
        }

        @Test
        public void skipUnsupportedColumns() throws Exception {
            String s = schemaName();
            tableTest(s, "SKIP_HANA")
                    .withArrow(useArrow())
                    .withOptions(opts -> opts.setSkipUnknownTypes(true))
                    .setupSql(
                        "CREATE TABLE " + s + ".SKIP_HANA ("
                        + "ID     INTEGER NOT NULL,"
                        + "NAME   NVARCHAR(50) NOT NULL,"
                        + "TAGS   INTEGER ARRAY,"
                        + "LABELS NVARCHAR(20) ARRAY,"
                        + "PRIMARY KEY (ID));"
                        + "INSERT INTO " + s + ".SKIP_HANA"
                        + " VALUES (1, 'a', ARRAY(1, 2, 3), ARRAY('x', 'y'));"
                        + "INSERT INTO " + s + ".SKIP_HANA"
                        + " VALUES (2, 'b', ARRAY(4, 5), ARRAY('z'))")
                    .cleanupSql("DROP TABLE " + s + ".SKIP_HANA CASCADE")
                    .expectPrimaryKey("ID")
                    .expectSkippedColumns("TAGS", "LABELS")
                    .expectRowCount(2)
                    .expectRowExists("ID", 1, "NAME", "a")
                    .run();
        }

        @Test
        public void clobContentImport() throws Exception {
            String s = schemaName();
            int size = 524288;
            String big = repeat("Z", size);
            tableTest(s, "CLOB_TBL")
                    .withArrow(useArrow())
                    .setupSql(
                        "CREATE TABLE " + s + ".CLOB_TBL ("
                        + "ID   INTEGER NOT NULL PRIMARY KEY,"
                        + "NOTE CLOB);"
                        + "INSERT INTO " + s + ".CLOB_TBL VALUES (2, NULL);"
                        + "INSERT INTO " + s + ".CLOB_TBL VALUES (3, '')")
                    .insertRow("INSERT INTO " + s + ".CLOB_TBL VALUES (?, ?)",
                            1, big)
                    .cleanupSql("DROP TABLE " + s + ".CLOB_TBL CASCADE")
                    .expectPrimaryKey("ID")
                    .expectRowCount(3)
                    .expectClobColumn("NOTE")
                    .expectClobContent("NOTE", "ID", 1, big)
                    .expectClobContent("NOTE", "ID", 2, null)
                    .expectClobContent("NOTE", "ID", 3, "")
                    .run();
        }

        private String repeat(String s, int n) {
            StringBuilder sb = new StringBuilder(s.length() * n);
            for (int i = 0; i < n; i++) {
                sb.append(s);
            }
            return sb.toString();
        }
    }

    @Nested class TableTestsRow extends HanaTableCases {
        @Override protected boolean useArrow() { return false; }
    }

    @Nested class TableTestsArrow extends HanaTableCases {
        @Override protected boolean useArrow() { return true; }
    }
}
