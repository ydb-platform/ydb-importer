package tech.ydb.importer.integration.sources.db2;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.TimeZone;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import tech.ydb.importer.config.SourceType;
import tech.ydb.importer.config.TableOptions.DateConv;
import tech.ydb.importer.integration.tabletest.AbstractYdbImporterTableTest;
import tech.ydb.importer.integration.typetest.AbstractYdbImporterTypeTest;
import tech.ydb.importer.integration.typetest.TypeTestBuilder;
import tech.ydb.table.values.DecimalType;
import tech.ydb.table.values.PrimitiveType;

/** IBM Db2 integration tests. */
public class Db2YdbImporterTest {

    private static Db2TestContainer db2Container;
    private static TimeZone originalTz;

    @BeforeAll
    static void startDb2() {
        originalTz = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        db2Container = new Db2TestContainer();
        db2Container.start();
    }

    @AfterAll
    static void stopDb2() {
        if (db2Container != null) {
            db2Container.stop();
            db2Container = null;
        }
        if (originalTz != null) {
            TimeZone.setDefault(originalTz);
        }
    }

    abstract class Db2TypeCases extends AbstractYdbImporterTypeTest {

        public abstract boolean useArrow();

        @Override
        public SourceDb sourceDb() {
            return new SourceDb(db2Container, SourceType.DB2, "IMPORT_TEST");
        }

        @Override
        protected TypeTestBuilder typeTest() {
            return super.typeTest().withIdentifierQuote("\"");
        }

        @Test
        public void smallintMapsToInt32() throws Exception {
            typeTest()
                    .column("SMALLINT NOT NULL", PrimitiveType.Int32)
                        .value("0", 0)
                        .value("32767", 32767)
                        .value("-32768", -32768)
                    .column("SMALLINT", PrimitiveType.Int32.makeOptional())
                        .value("42", 42)
                        .value("NULL", null)
                    .execute();
        }

        @Test
        public void integerBoundaries() throws Exception {
            typeTest()
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
            typeTest()
                    .column("BIGINT NOT NULL", PrimitiveType.Int64)
                        .value("0", 0L)
                        .value("9223372036854775807", Long.MAX_VALUE)
                        .value("-9223372036854775808", Long.MIN_VALUE)
                    .execute();
        }

        @Test
        public void realMapsToFloat() throws Exception {
            typeTest()
                    .column("REAL NOT NULL", PrimitiveType.Float)
                        .value("1.5", 1.5f)
                        .value("-0.25", -0.25f)
                    .execute();
        }

        @Test
        public void doubleMapsToDouble() throws Exception {
            typeTest()
                    .column("DOUBLE NOT NULL", PrimitiveType.Double)
                        .value("1.5", 1.5d)
                        .value("1e100", 1e100d)
                    .column("DOUBLE", PrimitiveType.Double.makeOptional())
                        .value("9.99", 9.99d)
                        .value("NULL", null)
                    .execute();
        }

        @Test
        public void decimalWithScale() throws Exception {
            typeTest()
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
            typeTest()
                    .column("DECIMAL(5, 0) NOT NULL", PrimitiveType.Int32)
                        .value("12345", 12345)
                        .value("-99999", -99999)
                    .execute();
        }

        @Test
        public void decimalZeroScaleBigToInt64() throws Exception {
            typeTest()
                    .column("DECIMAL(15, 0) NOT NULL", PrimitiveType.Int64)
                        .value("123456789012345", 123456789012345L)
                    .execute();
        }

        @Test
        public void varcharMaps() throws Exception {
            typeTest()
                    .column("VARCHAR(100) NOT NULL", PrimitiveType.Text)
                        .value("'hello'", "hello")
                        .value("''", "")
                        .value("'кириллица'", "кириллица")
                    .column("VARCHAR(100)",
                            PrimitiveType.Text.makeOptional())
                        .value("'hello'", "hello")
                        .value("NULL", null)
                    .execute();
        }

        @Test
        public void charPaddedToText() throws Exception {
            typeTest()
                    .column("CHAR(5) NOT NULL", PrimitiveType.Text)
                        .value("'abc'", "abc  ")
                        .value("'xyz'", "xyz  ")
                    .execute();
        }

        @Test
        public void varbinaryMapsToBytes() throws Exception {
            typeTest()
                    .column("VARBINARY(100) NOT NULL", PrimitiveType.Bytes)
                        .value("BX'DEADBEEF'",
                                new byte[]{(byte) 0xDE, (byte) 0xAD,
                                           (byte) 0xBE, (byte) 0xEF})
                        .value("BX'00'", new byte[]{(byte) 0x00})
                    .execute();
        }

        @Test
        public void varcharForBitDataMapsToBytes() throws Exception {
            typeTest()
                    .column("VARCHAR(100) FOR BIT DATA NOT NULL",
                            PrimitiveType.Bytes)
                        .value("BX'DEADBEEF'",
                                new byte[]{(byte) 0xDE, (byte) 0xAD,
                                           (byte) 0xBE, (byte) 0xEF})
                    .execute();
        }

        @Test
        public void booleanMaps() throws Exception {
            typeTest()
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
            typeTest()
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
            typeTest()
                    .withOptions(
                            opts -> opts.setDateConv(DateConv.INT))
                    .column("DATE NOT NULL", PrimitiveType.Int32)
                        .value("'1970-01-01'", 19700101)
                        .value("'2024-01-15'", 20240115)
                    .execute();
        }

        @Test
        public void timeMapsToInt32() throws Exception {
            typeTest()
                    .column("TIME NOT NULL", PrimitiveType.Int32)
                        .value("'00:00:00'", 0)
                        .value("'10:30:45'", 37845)
                        .value("'23:59:59'", 86399)
                    .execute();
        }

        @Test
        public void timestampDefaultMaps() throws Exception {
            // Default TIMESTAMP has fractional precision 6. Scale>0 picks Timestamp64.
            typeTest()
                    .column("TIMESTAMP NOT NULL", PrimitiveType.Timestamp64)
                        .value("'2024-01-15 10:30:45'",
                                java.time.Instant.parse(
                                        "2024-01-15T10:30:45Z"))
                        .value("'2024-01-15 10:30:45.123456'",
                                java.time.Instant.parse(
                                        "2024-01-15T10:30:45.123456Z"))
                    .execute();
        }

        @Test
        public void timestampZeroMaps() throws Exception {
            typeTest()
                    .column("TIMESTAMP(0) NOT NULL",
                            PrimitiveType.Datetime64)
                        .value("'2024-01-15 10:30:45'",
                                LocalDateTime.of(2024, 1, 15,
                                        10, 30, 45))
                    .execute();
        }
    }

    @Nested class TypeTestsRow extends Db2TypeCases {
        @Override public boolean useArrow() { return false; }
    }

    @Nested class TypeTestsArrow extends Db2TypeCases {
        @Override public boolean useArrow() { return true; }
    }

    abstract class Db2TableCases extends AbstractYdbImporterTableTest {

        private static final int BIG_BLOB_SEED = 4000;
        private static final int BIG_BLOB_SIZE = 512_000;

        public abstract boolean useArrow();

        @Override
        public SourceDb sourceDb() {
            return new SourceDb(db2Container, SourceType.DB2, "IMPORT_TEST");
        }

        @Override
        protected String smallBlob(byte[] data) {
            return "BLOB(X'" + hex(data) + "')";
        }

        @Override
        protected String bigBlob(String hexPair, int count) {
            StringBuilder sb = new StringBuilder("BLOB(X'");
            for (int i = 0; i < count; i++) {
                sb.append(hexPair);
            }
            sb.append("')");
            return sb.toString();
        }

        @Test
        public void primaryKeySelection() throws Exception {
            String s = schemaName();
            importTogether()
                    // 1. Composite PK
                    .add(tableTest(s, "PK_COMPOSITE")
                            .setupSql(
                                "CREATE TABLE " + s + ".PK_COMPOSITE ("
                                + "REGION_ID INTEGER NOT NULL,"
                                + "CITY_ID   INTEGER NOT NULL,"
                                + "NAME      VARCHAR(100) NOT NULL,"
                                + "PRIMARY KEY (REGION_ID, CITY_ID));"
                                + "INSERT INTO " + s + ".PK_COMPOSITE VALUES"
                                + "  (1, 10, 'Moscow');"
                                + "INSERT INTO " + s + ".PK_COMPOSITE VALUES"
                                + "  (1, 20, 'SPb');"
                                + "INSERT INTO " + s + ".PK_COMPOSITE VALUES"
                                + "  (2, 30, 'Berlin')")
                            .cleanupSql(
                                "DROP TABLE " + s + ".PK_COMPOSITE")
                            .expectPrimaryKey("REGION_ID", "CITY_ID")
                            .expectRowCount(3)
                            .expectRowExists(
                                    "REGION_ID", 1, "CITY_ID", 10,
                                    "NAME", "Moscow"))
                    // 2. PK preferred over UNIQUE constraint
                    .add(tableTest(s, "PK_OVER_UNIQUE")
                            .setupSql(
                                "CREATE TABLE " + s + ".PK_OVER_UNIQUE ("
                                + "ID   BIGINT NOT NULL PRIMARY KEY,"
                                + "CODE VARCHAR(20) NOT NULL,"
                                + "VAL  INTEGER,"
                                + "CONSTRAINT UQ_CODE UNIQUE (CODE));"
                                + "INSERT INTO " + s + ".PK_OVER_UNIQUE VALUES"
                                + "  (1, 'A', 10);"
                                + "INSERT INTO " + s + ".PK_OVER_UNIQUE VALUES"
                                + "  (2, 'B', 20)")
                            .cleanupSql(
                                "DROP TABLE " + s + ".PK_OVER_UNIQUE")
                            .expectPrimaryKey("ID")
                            .expectRowCount(2)
                            .expectRowExists("ID", 1L, "CODE", "A"))
                    // 3. UNIQUE constraint: fewer columns wins
                    .add(tableTest(s, "UQ_FEWER_COLS")
                            .setupSql(
                                "CREATE TABLE " + s + ".UQ_FEWER_COLS ("
                                + "A BIGINT NOT NULL,"
                                + "B VARCHAR(10) NOT NULL,"
                                + "C VARCHAR(10) NOT NULL,"
                                + "CONSTRAINT UQ_C UNIQUE (C),"
                                + "CONSTRAINT UQ_AB UNIQUE (A, B));"
                                + "INSERT INTO " + s + ".UQ_FEWER_COLS VALUES"
                                + "  (1, 'x', 'p');"
                                + "INSERT INTO " + s + ".UQ_FEWER_COLS VALUES"
                                + "  (2, 'y', 'q')")
                            .cleanupSql(
                                "DROP TABLE " + s + ".UQ_FEWER_COLS")
                            .expectPrimaryKey("C")
                            .expectRowCount(2)
                            .expectRowExists("C", "p"))
                    // 4. Bare CREATE UNIQUE INDEX (no constraint)
                    .add(tableTest(s, "UQ_INDEX_ONLY")
                            .setupSql(
                                "CREATE TABLE " + s + ".UQ_INDEX_ONLY ("
                                + "X BIGINT NOT NULL,"
                                + "A VARCHAR(10) NOT NULL,"
                                + "B VARCHAR(10) NOT NULL);"
                                + "CREATE UNIQUE INDEX " + s
                                + ".IDX_AB ON " + s + ".UQ_INDEX_ONLY (A, B);"
                                + "INSERT INTO " + s + ".UQ_INDEX_ONLY VALUES"
                                + "  (1, 'v1', 'v2');"
                                + "INSERT INTO " + s + ".UQ_INDEX_ONLY VALUES"
                                + "  (2, 'v4', 'v5')")
                            .cleanupSql(
                                "DROP TABLE " + s + ".UQ_INDEX_ONLY")
                            .expectPrimaryKey("A", "B")
                            .expectRowCount(2)
                            .expectRowExists("A", "v1", "B", "v2"))
                    .run();
        }

        @Test
        public void partitionedImport() throws Exception {
            String s = schemaName();
            tableTest(s, "PART_SALES")
                    
                    .setupSql(
                        "CREATE TABLE " + s + ".PART_SALES ("
                        + "SALE_ID   INTEGER NOT NULL,"
                        + "REGION_ID INTEGER NOT NULL,"
                        + "AMOUNT    INTEGER NOT NULL,"
                        + "PRIMARY KEY (SALE_ID, REGION_ID)"
                        + ") PARTITION BY RANGE (SALE_ID) ("
                        + "  STARTING FROM (1) ENDING AT (2) INCLUSIVE,"
                        + "  STARTING FROM (3) ENDING AT (4) INCLUSIVE,"
                        + "  STARTING FROM (5) ENDING AT (6) INCLUSIVE,"
                        + "  STARTING FROM (7) ENDING AT (8) INCLUSIVE);"
                        + "INSERT INTO " + s + ".PART_SALES VALUES (1, 10, 100);"
                        + "INSERT INTO " + s + ".PART_SALES VALUES (2, 20, 200);"
                        + "INSERT INTO " + s + ".PART_SALES VALUES (3, 30, 300);"
                        + "INSERT INTO " + s + ".PART_SALES VALUES (4, 40, 400);"
                        + "INSERT INTO " + s + ".PART_SALES VALUES (5, 50, 500);"
                        + "INSERT INTO " + s + ".PART_SALES VALUES (6, 60, 600);"
                        + "INSERT INTO " + s + ".PART_SALES VALUES (7, 70, 700);"
                        + "INSERT INTO " + s + ".PART_SALES VALUES (8, 80, 800)")
                    .cleanupSql(
                        "DROP TABLE " + s + ".PART_SALES")
                    .expectPrimaryKey("SALE_ID", "REGION_ID")
                    .expectRowCount(8)
                    .expectRowExists("SALE_ID", 1, "AMOUNT", 100)
                    .expectRowExists("SALE_ID", 8, "AMOUNT", 800)
                    .run();
        }

        @Test
        public void emptyTableCreatesSchema() throws Exception {
            String s = schemaName();
            tableTest(s, "EMPTY_TBL")
                    
                    .setupSql(
                        "CREATE TABLE " + s + ".EMPTY_TBL ("
                        + "ID   INTEGER NOT NULL,"
                        + "NAME VARCHAR(50) NOT NULL,"
                        + "PRIMARY KEY (ID))")
                    .cleanupSql("DROP TABLE " + s + ".EMPTY_TBL")
                    .expectPrimaryKey("ID")
                    .expectRowCount(0)
                    .run();
        }

        @Test
        public void customQueryTextImport() throws Exception {
            String s = schemaName();
            tableTest(s, "QUERY_SRC")
                    
                    .setupSql(
                        "CREATE TABLE " + s + ".QUERY_SRC ("
                        + "ID  INTEGER NOT NULL,"
                        + "VAL VARCHAR(50) NOT NULL,"
                        + "PRIMARY KEY (ID));"
                        + "INSERT INTO " + s + ".QUERY_SRC VALUES (1, 'a');"
                        + "INSERT INTO " + s + ".QUERY_SRC VALUES (2, 'b');"
                        + "INSERT INTO " + s + ".QUERY_SRC VALUES (3, 'c');"
                        + "INSERT INTO " + s + ".QUERY_SRC VALUES (4, 'd')")
                    .cleanupSql("DROP TABLE " + s + ".QUERY_SRC")
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
            String s = schemaName();
            tableTest(s, "SYNTH_TBL")
                    
                    .setupSql(
                        "CREATE TABLE " + s + ".SYNTH_TBL ("
                        + "SEQ  INTEGER NOT NULL,"
                        + "NAME VARCHAR(50) NOT NULL);"
                        + "INSERT INTO " + s + ".SYNTH_TBL VALUES (1, 'same');"
                        + "INSERT INTO " + s + ".SYNTH_TBL VALUES (2, 'same');"
                        + "INSERT INTO " + s + ".SYNTH_TBL VALUES (3, 'same')")
                    .cleanupSql("DROP TABLE " + s + ".SYNTH_TBL")
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
            tableTest(s, "SKIP_DB2")
                    
                    .withOptions(opts -> opts.setSkipUnknownTypes(true))
                    .setupSql(
                        "CREATE TABLE " + s + ".SKIP_DB2 ("
                        + "ID     INTEGER NOT NULL,"
                        + "NAME   VARCHAR(50) NOT NULL,"
                        + "DFL16  DECFLOAT(16),"
                        + "DFL34  DECFLOAT(34),"
                        + "PRIMARY KEY (ID));"
                        + "INSERT INTO " + s + ".SKIP_DB2"
                        + " VALUES (1, 'a', 1.5, 2.5);"
                        + "INSERT INTO " + s + ".SKIP_DB2"
                        + " VALUES (2, 'b', 3.5, 4.5)")
                    .cleanupSql("DROP TABLE " + s + ".SKIP_DB2")
                    .expectPrimaryKey("ID")
                    .expectSkippedColumns("DFL16", "DFL34")
                    .expectRowCount(2)
                    .expectRowExists("ID", 1, "NAME", "a")
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

            StringBuilder setup = new StringBuilder();
            setup.append("CREATE TABLE ").append(s).append(".MEGA_BLOB (")
                    .append("REGION    VARCHAR(10) NOT NULL,")
                    .append("LABEL     VARCHAR(30),")
                    .append("THUMBNAIL BLOB,")
                    .append("FULLSIZE  BLOB")
                    .append(") PARTITION BY RANGE (REGION) (")
                    .append("STARTING FROM ('EU') ENDING AT ('EU') INCLUSIVE,")
                    .append("STARTING FROM ('US') ENDING AT ('US') INCLUSIVE)");
            appendMegaBlobInsert(setup, s, "EU", "eu-book-1", thumbEU1, "11");
            appendMegaBlobInsert(setup, s, "EU", "eu-book-2", thumbEU2, "22");
            setup.append(";INSERT INTO ").append(s).append(".MEGA_BLOB")
                    .append(" VALUES ('EU', 'eu-null', NULL, NULL)");
            appendMegaBlobInsert(setup, s, "US", "us-book-1", thumbUS1, "33");
            appendMegaBlobInsert(setup, s, "US", "us-book-2", thumbUS2, "44");
            for (int i = 0; i < 7; i++) {
                setup.append(";UPDATE ").append(s).append(".MEGA_BLOB")
                        .append(" SET FULLSIZE = FULLSIZE || FULLSIZE")
                        .append(" WHERE FULLSIZE IS NOT NULL");
            }

            tableTest(s, "MEGA_BLOB")
                    
                    .setupSql(setup.toString())
                    .cleanupSql("DROP TABLE " + s + ".MEGA_BLOB")
                    .expectSyntheticKey()
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
                    
                    .setupSql(
                        "CREATE TABLE " + s + ".SMALL_BLOB ("
                        + "ID   INTEGER NOT NULL PRIMARY KEY,"
                        + "NAME VARCHAR(50) NOT NULL,"
                        + "DATA BLOB);"
                        + "INSERT INTO " + s + ".SMALL_BLOB VALUES"
                        + "  (1, 'hello',     " + smallBlob("Hello".getBytes()) + ");"
                        + "INSERT INTO " + s + ".SMALL_BLOB VALUES"
                        + "  (2, 'null_blob', NULL);"
                        + "INSERT INTO " + s + ".SMALL_BLOB VALUES"
                        + "  (3, 'world',     " + smallBlob("World".getBytes()) + ")")
                    .cleanupSql("DROP TABLE " + s + ".SMALL_BLOB")
                    .expectPrimaryKey("ID")
                    .expectRowCount(3)
                    .expectBlobColumn("DATA")
                    .expectBlobBytes("DATA", "ID", 1, "Hello".getBytes())
                    .expectBlobBytes("DATA", "ID", 3, "World".getBytes())
                    .expectBlobBytes("DATA", "ID", 2, null)
                    .run();
        }

        private void appendMegaBlobInsert(StringBuilder sb, String schema,
                String region, String label, byte[] thumb, String hexPair) {
            byte[] seed = filled(BIG_BLOB_SEED, (byte) Integer.parseInt(hexPair, 16));
            sb.append(";INSERT INTO ").append(schema).append(".MEGA_BLOB VALUES ('")
                    .append(region).append("', '").append(label).append("', ")
                    .append(smallBlob(thumb)).append(", ")
                    .append("BLOB(X'").append(hex(seed)).append("'))");
        }

        @Test
        public void clobContentImport() throws Exception {
            String s = schemaName();
            int size = 524288;
            String big = repeat("Z", size);
            tableTest(s, "CLOB_TBL")
                    
                    .setupSql(
                        "CREATE TABLE " + s + ".CLOB_TBL ("
                        + "ID   INTEGER NOT NULL PRIMARY KEY,"
                        + "NOTE CLOB);"
                        + "INSERT INTO " + s + ".CLOB_TBL VALUES (2, NULL);"
                        + "INSERT INTO " + s + ".CLOB_TBL VALUES (3, '')")
                    .insertRow("INSERT INTO " + s + ".CLOB_TBL VALUES (?, ?)",
                            1, big)
                    .cleanupSql("DROP TABLE " + s + ".CLOB_TBL")
                    .expectPrimaryKey("ID")
                    .expectRowCount(3)
                    .expectClobColumn("NOTE")
                    .expectClobContent("NOTE", "ID", 1, big)
                    .expectClobContent("NOTE", "ID", 2, null)
                    .expectClobContent("NOTE", "ID", 3, "")
                    .run();
        }
    }

    @Nested class TableTestsRow extends Db2TableCases {
        @Override public boolean useArrow() { return false; }
    }

    @Nested class TableTestsArrow extends Db2TableCases {
        @Override public boolean useArrow() { return true; }
    }
}
