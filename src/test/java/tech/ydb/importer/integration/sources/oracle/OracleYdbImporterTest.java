package tech.ydb.importer.integration.sources.oracle;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.TimeZone;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.testcontainers.oracle.OracleContainer;

import tech.ydb.importer.config.SourceType;
import tech.ydb.importer.integration.tabletest.AbstractYdbImporterTableTest;
import tech.ydb.importer.integration.typetest.AbstractYdbImporterTypeTest;
import tech.ydb.importer.integration.typetest.TypeTestBuilder;
import tech.ydb.table.values.DecimalType;
import tech.ydb.table.values.PrimitiveType;

/** Oracle integration tests. */
public class OracleYdbImporterTest {

    private static OracleContainer oracleContainer;
    private static TimeZone originalTz;

    private static String schemaName;

    @BeforeAll
    static void startOracle() {
        originalTz = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        oracleContainer = OracleTestContainer.create();
        oracleContainer.start();
        schemaName = oracleContainer.getUsername().toUpperCase();
    }

    @AfterAll
    static void stopOracle() {
        if (oracleContainer != null) {
            oracleContainer.stop();
            oracleContainer = null;
        }
        if (originalTz != null) {
            TimeZone.setDefault(originalTz);
        }
    }

    abstract class OracleTypeCases extends AbstractYdbImporterTypeTest {


        @Override
        public SourceDb sourceDb() {
            return new SourceDb(oracleContainer, SourceType.ORACLE,
                    schemaName);
        }

        @Override
        protected TypeTestBuilder typeTest() {
            return super.typeTest().withIdentifierQuote("\"");
        }

        @Test
        public void numberSmallToInt32() throws Exception {
            typeTest()
                    .column("NUMBER(5,0) NOT NULL", PrimitiveType.Int32)
                        .value("0", 0)
                        .value("99999", 99999)
                        .value("-99999", -99999)
                    .column("NUMBER(9,0)", PrimitiveType.Int32.makeOptional())
                        .value("42", 42)
                        .value("NULL", null)
                    .execute();
        }

        @Test
        public void numberMediumToInt64() throws Exception {
            typeTest()
                    .column("NUMBER(10,0) NOT NULL", PrimitiveType.Int64)
                        .value("0", 0L)
                        .value("9999999999", 9999999999L)
                    .execute();
        }

        @Test
        public void numberLargeToInt64() throws Exception {
            typeTest()
                    .column("NUMBER(19,0) NOT NULL", PrimitiveType.Int64)
                        .value("1234567890123456789",
                                1234567890123456789L)
                    .execute();
        }

        @Test
        public void numberWithScale() throws Exception {
            typeTest()
                    .column("NUMBER(10,2) NOT NULL",
                            DecimalType.of(10, 2))
                        .value("12345.67",
                                new BigDecimal("12345.67"))
                        .value("-99999.99",
                                new BigDecimal("-99999.99"))
                    .column("NUMBER(10,4)",
                            DecimalType.of(10, 4).makeOptional())
                        .value("123.4567",
                                new BigDecimal("123.4567"))
                        .value("NULL", null)
                    .execute();
        }

        @Test
        public void bareNumberToDouble() throws Exception {
            typeTest()
                    .column("NUMBER NOT NULL", PrimitiveType.Double)
                        .value("1.5", 1.5d)
                        .value("12345", 12345.0d)
                    .execute();
        }

        @Test
        public void binaryFloatMapsToFloat() throws Exception {
            typeTest()
                    .column("BINARY_FLOAT NOT NULL", PrimitiveType.Float)
                        .value("1.5", 1.5f)
                        .value("-0.25", -0.25f)
                    .execute();
        }

        @Test
        public void binaryDoubleMapsToDouble() throws Exception {
            typeTest()
                    .column("BINARY_DOUBLE NOT NULL", PrimitiveType.Double)
                        .value("1.5", 1.5d)
                        .value("1e100", 1e100d)
                    .column("BINARY_DOUBLE",
                            PrimitiveType.Double.makeOptional())
                        .value("9.99", 9.99d)
                        .value("NULL", null)
                    .execute();
        }

        @Test
        public void varchar2Maps() throws Exception {
            typeTest()
                    .column("VARCHAR2(100) NOT NULL", PrimitiveType.Text)
                        .value("'hello'", "hello")
                        .value("'кириллица'", "кириллица")
                    .column("VARCHAR2(100)",
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
                        .value("'xy'", "xy   ")
                    .execute();
        }

        @Test
        public void rawMapsToBytes() throws Exception {
            typeTest()
                    .column("RAW(100) NOT NULL", PrimitiveType.Bytes)
                        .value("HEXTORAW('DEADBEEF')",
                                new byte[]{(byte) 0xDE, (byte) 0xAD,
                                           (byte) 0xBE, (byte) 0xEF})
                        .value("HEXTORAW('00')",
                                new byte[]{(byte) 0x00})
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
        public void dateMapsToDatetime64() throws Exception {
            typeTest()
                    .column("DATE NOT NULL", PrimitiveType.Datetime64)
                        .value("TO_DATE('2024-01-15 10:30:45',"
                                + "'YYYY-MM-DD HH24:MI:SS')",
                                LocalDateTime.of(2024, 1, 15,
                                        10, 30, 45))
                        .value("TO_DATE('1970-01-01 00:00:00',"
                                + "'YYYY-MM-DD HH24:MI:SS')",
                                LocalDateTime.of(1970, 1, 1, 0, 0, 0))
                    .execute();
        }

        @Test
        public void timestampDefaultMaps() throws Exception {
            typeTest()
                    .column("TIMESTAMP NOT NULL",
                            PrimitiveType.Timestamp64)
                        .value("TO_TIMESTAMP('2024-01-15 10:30:45',"
                                + "'YYYY-MM-DD HH24:MI:SS')",
                                java.time.Instant.parse(
                                        "2024-01-15T10:30:45Z"))
                        .value("TO_TIMESTAMP('2024-01-15 10:30:45.123456',"
                                + "'YYYY-MM-DD HH24:MI:SS.FF')",
                                java.time.Instant.parse(
                                    "2024-01-15T10:30:45.123456Z"))
                    .execute();
        }

        @Test
        public void timestampZeroMaps() throws Exception {
            // TIMESTAMP(0) -> scale=0 -> Datetime64.
            typeTest()
                    .column("TIMESTAMP(0) NOT NULL",
                            PrimitiveType.Datetime64)
                        .value("TO_TIMESTAMP('2024-01-15 10:30:45',"
                                + "'YYYY-MM-DD HH24:MI:SS')",
                                LocalDateTime.of(2024, 1, 15,
                                        10, 30, 45))
                    .execute();
        }
    }

    @Nested class TypeTestsRow extends OracleTypeCases {
        @Override public boolean useArrow() { return false; }
    }

    @Nested class TypeTestsArrow extends OracleTypeCases {
        @Override public boolean useArrow() { return true; }
    }

    abstract class OracleTableCases extends AbstractYdbImporterTableTest {

        private static final int BIG_BLOB_SIZE = 512_000;


        @Override
        public SourceDb sourceDb() {
            return new SourceDb(oracleContainer, SourceType.ORACLE,
                    schemaName);
        }

        @Override
        protected String smallBlob(byte[] data) {
            return "TO_BLOB(HEXTORAW('" + hex(data) + "'))";
        }

        // Same as base, but identifiers are UPPERCASE.
        @Test
        public void primaryKeySelection() throws Exception {
            String s = schemaName();
            importTogether()
                    .add(tableTest(s, "PK_COMPOSITE")
                            .setupSql(
                                "CREATE TABLE " + s + ".PK_COMPOSITE ("
                                + "REGION_ID NUMBER(10,0) NOT NULL,"
                                + "CITY_ID   NUMBER(10,0) NOT NULL,"
                                + "NAME      VARCHAR2(100) NOT NULL,"
                                + "PRIMARY KEY (REGION_ID, CITY_ID));"
                                + "INSERT INTO " + s + ".PK_COMPOSITE"
                                + " VALUES (1, 10, 'Moscow');"
                                + "INSERT INTO " + s + ".PK_COMPOSITE"
                                + " VALUES (1, 20, 'SPb');"
                                + "INSERT INTO " + s + ".PK_COMPOSITE"
                                + " VALUES (2, 30, 'Berlin')")
                            .cleanupSql(
                                "DROP TABLE " + s + ".PK_COMPOSITE")
                            .expectPrimaryKey("REGION_ID", "CITY_ID")
                            .expectRowCount(3)
                            .expectRowExists(
                                    "REGION_ID", 1L, "CITY_ID", 10L,
                                    "NAME", "Moscow"))
                    .add(tableTest(s, "PK_OVER_UNIQUE")
                            .setupSql(
                                "CREATE TABLE " + s + ".PK_OVER_UNIQUE ("
                                + "ID   NUMBER(19,0) NOT NULL PRIMARY KEY,"
                                + "CODE VARCHAR2(20) NOT NULL,"
                                + "VAL  NUMBER(10,0),"
                                + "CONSTRAINT UQ_ORA_CODE UNIQUE (CODE));"
                                + "INSERT INTO " + s + ".PK_OVER_UNIQUE"
                                + " VALUES (1, 'A', 10);"
                                + "INSERT INTO " + s + ".PK_OVER_UNIQUE"
                                + " VALUES (2, 'B', 20)")
                            .cleanupSql(
                                "DROP TABLE " + s + ".PK_OVER_UNIQUE")
                            .expectPrimaryKey("ID")
                            .expectRowCount(2)
                            .expectRowExists("ID", 1L, "CODE", "A"))
                    .add(tableTest(s, "UQ_FEWER_COLS")
                            .setupSql(
                                "CREATE TABLE " + s + ".UQ_FEWER_COLS ("
                                + "A NUMBER(19,0) NOT NULL,"
                                + "B VARCHAR2(10) NOT NULL,"
                                + "C VARCHAR2(10) NOT NULL,"
                                + "CONSTRAINT UQ_ORA_C UNIQUE (C),"
                                + "CONSTRAINT UQ_ORA_AB UNIQUE (A, B));"
                                + "INSERT INTO " + s + ".UQ_FEWER_COLS"
                                + " VALUES (1, 'x', 'p');"
                                + "INSERT INTO " + s + ".UQ_FEWER_COLS"
                                + " VALUES (2, 'y', 'q')")
                            .cleanupSql(
                                "DROP TABLE " + s + ".UQ_FEWER_COLS")
                            .expectPrimaryKey("C")
                            .expectRowCount(2)
                            .expectRowExists("C", "p"))
                    .run();
        }

        @Test
        public void emptyTableCreatesSchema() throws Exception {
            String s = schemaName();
            tableTest(s, "EMPTY_TBL")
                    .setupSql(
                        "CREATE TABLE " + s + ".EMPTY_TBL ("
                        + "ID   NUMBER(10,0) NOT NULL,"
                        + "NAME VARCHAR2(50) NOT NULL,"
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
                        + "ID  NUMBER(10,0) NOT NULL,"
                        + "VAL VARCHAR2(50) NOT NULL,"
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
                    .expectRowExists("ID", 1L, "VAL", "a")
                    .expectRowExists("ID", 2L, "VAL", "b")
                    .run();
        }

        @Test
        public void syntheticKeyDistinguishesRows() throws Exception {
            String s = schemaName();
            tableTest(s, "SYNTH_TBL")
                    .setupSql(
                        "CREATE TABLE " + s + ".SYNTH_TBL ("
                        + "SEQ  NUMBER(10,0) NOT NULL,"
                        + "NAME VARCHAR2(50) NOT NULL);"
                        + "INSERT INTO " + s + ".SYNTH_TBL VALUES (1, 'same');"
                        + "INSERT INTO " + s + ".SYNTH_TBL VALUES (2, 'same');"
                        + "INSERT INTO " + s + ".SYNTH_TBL VALUES (3, 'same')")
                    .cleanupSql("DROP TABLE " + s + ".SYNTH_TBL")
                    .expectSyntheticKey()
                    .expectRowCount(3)
                    .expectRowExists("SEQ", 1L, "NAME", "same")
                    .expectRowExists("SEQ", 2L, "NAME", "same")
                    .expectRowExists("SEQ", 3L, "NAME", "same")
                    .run();
        }

        @Test
        public void skipUnsupportedColumns() throws Exception {
            /*
             * TIMESTAMP WITH TIME ZONE -> vendor type -101, not handled.
             * INTERVAL DAY TO SECOND -> vendor type -104, not handled.
             */
            String s = schemaName();
            tableTest(s, "SKIP_ORA")
                    .withOptions(opts -> opts.setSkipUnknownTypes(true))
                    .setupSql(
                        "CREATE TABLE " + s + ".SKIP_ORA ("
                        + "ID   NUMBER(10,0) NOT NULL,"
                        + "NAME VARCHAR2(50) NOT NULL,"
                        + "TSZ  TIMESTAMP WITH TIME ZONE,"
                        + "IDS  INTERVAL DAY TO SECOND,"
                        + "PRIMARY KEY (ID));"
                        + "INSERT INTO " + s + ".SKIP_ORA"
                        + " (ID, NAME, TSZ, IDS) VALUES"
                        + " (1, 'a',"
                        + "  TIMESTAMP '2024-01-15 10:30:45 +00:00',"
                        + "  INTERVAL '1 12:00:00' DAY TO SECOND);"
                        + "INSERT INTO " + s + ".SKIP_ORA"
                        + " (ID, NAME, TSZ, IDS) VALUES"
                        + " (2, 'b',"
                        + "  TIMESTAMP '2024-06-01 08:00:00 +03:00',"
                        + "  INTERVAL '2 00:00:00' DAY TO SECOND)")
                    .cleanupSql("DROP TABLE " + s + ".SKIP_ORA")
                    .expectPrimaryKey("ID")
                    .expectSkippedColumns("TSZ", "IDS")
                    .expectRowCount(2)
                    .expectRowExists("ID", 1L, "NAME", "a")
                    .run();
        }

        @Test
        public void partitionedImport() throws Exception {
            String s = schemaName();
            tableTest(s, "PART_SALES")
                    .setupSql(
                        "CREATE TABLE " + s + ".PART_SALES ("
                        + "SALE_ID   NUMBER(10,0) NOT NULL,"
                        + "REGION_ID NUMBER(10,0) NOT NULL,"
                        + "AMOUNT    NUMBER(10,0) NOT NULL,"
                        + "PRIMARY KEY (SALE_ID, REGION_ID)"
                        + ") PARTITION BY RANGE (SALE_ID) ("
                        + "  PARTITION P1 VALUES LESS THAN (3),"
                        + "  PARTITION P2 VALUES LESS THAN (5),"
                        + "  PARTITION P3 VALUES LESS THAN (7),"
                        + "  PARTITION P4 VALUES LESS THAN (MAXVALUE));"
                        + "INSERT INTO " + s + ".PART_SALES VALUES (1, 10, 100);"
                        + "INSERT INTO " + s + ".PART_SALES VALUES (2, 20, 200);"
                        + "INSERT INTO " + s + ".PART_SALES VALUES (3, 30, 300);"
                        + "INSERT INTO " + s + ".PART_SALES VALUES (4, 40, 400);"
                        + "INSERT INTO " + s + ".PART_SALES VALUES (5, 50, 500);"
                        + "INSERT INTO " + s + ".PART_SALES VALUES (6, 60, 600);"
                        + "INSERT INTO " + s + ".PART_SALES VALUES (7, 70, 700);"
                        + "INSERT INTO " + s + ".PART_SALES VALUES (8, 80, 800)")
                    .cleanupSql("DROP TABLE " + s + ".PART_SALES")
                    .expectPrimaryKey("SALE_ID", "REGION_ID")
                    .expectRowCount(8)
                    .expectRowExists("SALE_ID", 1L, "AMOUNT", 100L)
                    .expectRowExists("SALE_ID", 8L, "AMOUNT", 800L)
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
                    .setupSql(
                        "CREATE TABLE " + s + ".MEGA_BLOB ("
                        + "REGION    VARCHAR2(10) NOT NULL,"
                        + "LABEL     VARCHAR2(30),"
                        + "THUMBNAIL BLOB,"
                        + "FULLSIZE  BLOB"
                        + ") PARTITION BY LIST (REGION) ("
                        + "PARTITION P_EU VALUES ('EU'),"
                        + "PARTITION P_US VALUES ('US'))")
                    .insertRow(insertSql, "EU", "eu-book-1", thumbEU1, fullEU1)
                    .insertRow(insertSql, "EU", "eu-book-2", thumbEU2, fullEU2)
                    .insertRow(insertSql, "EU", "eu-null",   null,     null)
                    .insertRow(insertSql, "US", "us-book-1", thumbUS1, fullUS1)
                    .insertRow(insertSql, "US", "us-book-2", thumbUS2, fullUS2)
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
                        + "ID   NUMBER(10,0) NOT NULL PRIMARY KEY,"
                        + "NAME VARCHAR2(50) NOT NULL,"
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
                    .expectBlobBytes("DATA", "ID", 1L, "Hello".getBytes())
                    .expectBlobBytes("DATA", "ID", 3L, "World".getBytes())
                    .expectBlobBytes("DATA", "ID", 2L, null)
                    .run();
        }

        @Test
        public void rangeSplitsByColumnType() throws Exception {
            String s = schemaName();
            importTogether()
                    .add(tableTest(s, "SPLIT_INT")
                            .setupSql(
                                "CREATE TABLE " + s + ".SPLIT_INT ("
                                + "ID  NUMBER(10,0) NOT NULL PRIMARY KEY,"
                                + "VAL NUMBER(10,0) NOT NULL);"
                                + "INSERT INTO " + s + ".SPLIT_INT VALUES (1,5);"
                                + "INSERT INTO " + s + ".SPLIT_INT VALUES (2,12);"
                                + "INSERT INTO " + s + ".SPLIT_INT VALUES (3,28);"
                                + "INSERT INTO " + s + ".SPLIT_INT VALUES (4,45);"
                                + "INSERT INTO " + s + ".SPLIT_INT VALUES (5,60);"
                                + "INSERT INTO " + s + ".SPLIT_INT VALUES (6,73);"
                                + "INSERT INTO " + s + ".SPLIT_INT VALUES (7,88);"
                                + "INSERT INTO " + s + ".SPLIT_INT VALUES (8,99)")
                            .cleanupSql("DROP TABLE " + s + ".SPLIT_INT")
                            .splitBy("VAL").splitFrom("0").splitTo("100").splitCount(4)
                            .expectPrimaryKey("ID")
                            .expectRowCount(8))
                    .add(tableTest(s, "SPLIT_DEC")
                            .setupSql(
                                "CREATE TABLE " + s + ".SPLIT_DEC ("
                                + "ID  NUMBER(10,0) NOT NULL PRIMARY KEY,"
                                + "VAL NUMBER(10,2) NOT NULL);"
                                + "INSERT INTO " + s + ".SPLIT_DEC VALUES (1,1.50);"
                                + "INSERT INTO " + s + ".SPLIT_DEC VALUES (2,10.25);"
                                + "INSERT INTO " + s + ".SPLIT_DEC VALUES (3,33.75);"
                                + "INSERT INTO " + s + ".SPLIT_DEC VALUES (4,50.00);"
                                + "INSERT INTO " + s + ".SPLIT_DEC VALUES (5,75.50);"
                                + "INSERT INTO " + s + ".SPLIT_DEC VALUES (6,99.99)")
                            .cleanupSql("DROP TABLE " + s + ".SPLIT_DEC")
                            .splitBy("VAL").splitFrom("0").splitTo("100").splitCount(3)
                            .expectPrimaryKey("ID")
                            .expectRowCount(6))
                    .add(tableTest(s, "SPLIT_DBL")
                            .setupSql(
                                "CREATE TABLE " + s + ".SPLIT_DBL ("
                                + "ID  NUMBER(10,0) NOT NULL PRIMARY KEY,"
                                + "VAL BINARY_DOUBLE NOT NULL);"
                                + "INSERT INTO " + s + ".SPLIT_DBL VALUES (1,0.1);"
                                + "INSERT INTO " + s + ".SPLIT_DBL VALUES (2,1.5);"
                                + "INSERT INTO " + s + ".SPLIT_DBL VALUES (3,3.7);"
                                + "INSERT INTO " + s + ".SPLIT_DBL VALUES (4,5.0);"
                                + "INSERT INTO " + s + ".SPLIT_DBL VALUES (5,7.2)")
                            .cleanupSql("DROP TABLE " + s + ".SPLIT_DBL")
                            .splitBy("VAL").splitFrom("0").splitTo("10").splitCount(3)
                            .expectPrimaryKey("ID")
                            .expectRowCount(5))
                    .add(tableTest(s, "SPLIT_DATE")
                            .setupSql(
                                "CREATE TABLE " + s + ".SPLIT_DATE ("
                                + "ID  NUMBER(10,0) NOT NULL PRIMARY KEY,"
                                + "VAL DATE NOT NULL);"
                                + "INSERT INTO " + s + ".SPLIT_DATE VALUES"
                                + "  (1, DATE '2024-01-15');"
                                + "INSERT INTO " + s + ".SPLIT_DATE VALUES"
                                + "  (2, DATE '2024-03-10');"
                                + "INSERT INTO " + s + ".SPLIT_DATE VALUES"
                                + "  (3, DATE '2024-06-01');"
                                + "INSERT INTO " + s + ".SPLIT_DATE VALUES"
                                + "  (4, DATE '2024-09-20');"
                                + "INSERT INTO " + s + ".SPLIT_DATE VALUES"
                                + "  (5, DATE '2024-12-25')")
                            .cleanupSql("DROP TABLE " + s + ".SPLIT_DATE")
                            .splitBy("VAL")
                                .splitFrom("2024-01-01").splitTo("2025-01-01")
                                .splitCount(4)
                            .expectPrimaryKey("ID")
                            .expectRowCount(5))
                    .add(tableTest(s, "SPLIT_TS")
                            .setupSql(
                                "CREATE TABLE " + s + ".SPLIT_TS ("
                                + "ID  NUMBER(10,0) NOT NULL PRIMARY KEY,"
                                + "VAL TIMESTAMP NOT NULL);"
                                + "INSERT INTO " + s + ".SPLIT_TS VALUES"
                                + "  (1, TIMESTAMP '2024-01-15 10:00:00');"
                                + "INSERT INTO " + s + ".SPLIT_TS VALUES"
                                + "  (2, TIMESTAMP '2024-06-01 14:30:00');"
                                + "INSERT INTO " + s + ".SPLIT_TS VALUES"
                                + "  (3, TIMESTAMP '2024-12-25 23:59:59')")
                            .cleanupSql("DROP TABLE " + s + ".SPLIT_TS")
                            .splitBy("VAL")
                                .splitFrom("2024-01-01 00:00:00")
                                .splitTo("2025-01-01 00:00:00")
                                .splitCount(3)
                            .expectPrimaryKey("ID")
                            .expectRowCount(3))
                    .run();
        }

        @Test
        public void clobContentImport() throws Exception {
            String s = sourceDb().schema;
            String bigText = repeat("Y", 524288);

            tableTest(s, "CLOB_DOCS")
                    .setupSql(
                        "CREATE TABLE " + s + ".CLOB_DOCS ("
                        + "ID    NUMBER(10,0) NOT NULL PRIMARY KEY,"
                        + "SHORT CLOB,"
                        + "LARGE CLOB);"
                        + "INSERT INTO " + s + ".CLOB_DOCS VALUES (3, EMPTY_CLOB(), EMPTY_CLOB())")
                    .insertRow("INSERT INTO " + s + ".CLOB_DOCS VALUES (?, ?, ?)",
                            1, "YDB", bigText)
                    .insertRow("INSERT INTO " + s + ".CLOB_DOCS VALUES (?, ?, ?)",
                            2, null, null)
                    .cleanupSql("DROP TABLE " + s + ".CLOB_DOCS")
                    .expectPrimaryKey("ID")
                    .expectRowCount(3)
                    .expectClobColumn("SHORT")
                    .expectClobColumn("LARGE")
                    .expectClobContent("SHORT", "ID", 1, "YDB")
                    .expectClobContent("LARGE", "ID", 1, bigText)
                    .expectClobContent("SHORT", "ID", 2, null)
                    .expectClobContent("LARGE", "ID", 2, null)
                    .expectClobContent("SHORT", "ID", 3, "")
                    .expectClobContent("LARGE", "ID", 3, "")
                    .run();
        }

    }

    @Nested class TableTestsRow extends OracleTableCases {
        @Override public boolean useArrow() { return false; }
    }

    @Nested class TableTestsArrow extends OracleTableCases {
        @Override public boolean useArrow() { return true; }
    }

    @Nested class PartitioningTests extends AbstractOraclePartitioningTests {
        @Override
        public SourceDb sourceDb() {
            return new SourceDb(oracleContainer, SourceType.ORACLE, schemaName);
        }
    }
}
