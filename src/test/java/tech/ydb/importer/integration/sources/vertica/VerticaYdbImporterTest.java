package tech.ydb.importer.integration.sources.vertica;

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

/** Vertica integration tests. */
public class VerticaYdbImporterTest {

    private static VerticaTestContainer verticaContainer;
    private static TimeZone originalTz;

    @BeforeAll
    static void startVertica() {
        originalTz = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        verticaContainer = new VerticaTestContainer();
        verticaContainer.start();
    }

    @AfterAll
    static void stopVertica() {
        if (verticaContainer != null) {
            verticaContainer.stop();
            verticaContainer = null;
        }
        if (originalTz != null) {
            TimeZone.setDefault(originalTz);
        }
    }

    abstract class VerticaTypeCases extends AbstractYdbImporterTypeTest {

        public abstract boolean useArrow();

        @Override
        public SourceDb sourceDb() {
            return new SourceDb(verticaContainer, SourceType.VERTICA, "public");
        }

        @Override
        protected String createTableSuffix() {
            return "";
        }

        @Test
        public void integerMapsToInt64() throws Exception {
            typeTest()
                    .column("INT NOT NULL", PrimitiveType.Int64)
                        .value("0", 0L)
                        .value("-1", -1L)
                        .value("2147483647", 2147483647L)
                    .column("INT", PrimitiveType.Int64.makeOptional())
                        .value("42", 42L)
                        .value("NULL", null)
                    .execute();
        }

        @Test
        public void smallintAndTinyintMapToInt64() throws Exception {
            typeTest()
                    .column("SMALLINT NOT NULL", PrimitiveType.Int64)
                        .value("32767", 32767L)
                        .value("-32767", -32767L)
                    .column("TINYINT NOT NULL", PrimitiveType.Int64)
                        .value("127", 127L)
                        .value("-127", -127L)
                    .execute();
        }

        @Test
        public void bigintBoundaries() throws Exception {
            typeTest()
                    .column("BIGINT NOT NULL", PrimitiveType.Int64)
                        .value("0", 0L)
                        .value("9223372036854775807", Long.MAX_VALUE)
                        .value("-9223372036854775807", -Long.MAX_VALUE)
                    .execute();
        }

        @Test
        public void booleanMaps() throws Exception {
            typeTest()
                    .column("BOOLEAN NOT NULL", PrimitiveType.Bool)
                        .value("TRUE", true)
                        .value("FALSE", false)
                    .column("BOOLEAN",
                            PrimitiveType.Bool.makeOptional())
                        .value("TRUE", true)
                        .value("NULL", null)
                    .execute();
        }

        @Test
        public void doublePrecision() throws Exception {
            typeTest()
                    .column("DOUBLE PRECISION NOT NULL",
                            PrimitiveType.Double)
                        .value("1.5", 1.5d)
                        .value("-0.25", -0.25d)
                    .column("FLOAT NOT NULL", PrimitiveType.Double)
                        .value("3.14", 3.14d)
                        .value("2.71", 2.71d)
                    .column("DOUBLE PRECISION",
                            PrimitiveType.Double.makeOptional())
                        .value("9.99", 9.99d)
                        .value("NULL", null)
                    .execute();
        }

        @Test
        public void realMapsToDouble() throws Exception {
            typeTest()
                    .column("REAL NOT NULL", PrimitiveType.Double)
                        .value("1.5", 1.5d)
                        .value("1e100", 1e100d)
                    .execute();
        }

        @Test
        public void stringTypesMapToText() throws Exception {
            typeTest()
                    .column("VARCHAR(100) NOT NULL", PrimitiveType.Text)
                        .value("'hello'", "hello")
                        .value("''", "")
                        .value("'кириллица'", "кириллица")
                    .column("LONG VARCHAR NOT NULL", PrimitiveType.Text)
                        .value("'long text'", "long text")
                        .value("'second'", "second")
                        .value("'third'", "third")
                    .column("VARCHAR(100)",
                            PrimitiveType.Text.makeOptional())
                        .value("'hello'", "hello")
                        .value("NULL", null)
                    .execute();
        }

        @Test
        public void charPreservesTrailingSpaces() throws Exception {
            typeTest()
                    .column("CHAR(10) NOT NULL", PrimitiveType.Text)
                        .value("'hello'", "hello     ")
                    .execute();
        }

        @Test
        public void varbinaryMapsToBytes() throws Exception {
            typeTest()
                    .column("VARBINARY(100) NOT NULL",
                            PrimitiveType.Bytes)
                        .value("HEX_TO_BINARY('0xDEADBEEF')",
                                new byte[]{(byte) 0xDE, (byte) 0xAD,
                                           (byte) 0xBE, (byte) 0xEF})
                        .value("HEX_TO_BINARY('0x00')",
                                new byte[]{(byte) 0x00})
                    .execute();
        }

        @Test
        public void scaledDecimalMapsToDecimal() throws Exception {
            typeTest()
                    .column("NUMERIC(10, 4) NOT NULL",
                            DecimalType.of(10, 4))
                        .value("123.4567", new BigDecimal("123.4567"))
                        .value("-999999.9999",
                                new BigDecimal("-999999.9999"))
                        .value("0", new BigDecimal("0.0000"))
                    .column("NUMERIC(10, 2)",
                            DecimalType.of(10, 2).makeOptional())
                        .value("99.99", new BigDecimal("99.99"))
                        .value("NULL", null)
                    .execute();
        }

        @Test
        public void zeroScaleDecimalMapsToInt32() throws Exception {
            typeTest()
                    .column("NUMERIC(5, 0) NOT NULL",
                            PrimitiveType.Int32)
                        .value("12345", 12345)
                        .value("-99999", -99999)
                    .execute();
        }

        @Test
        public void zeroScaleDecimalMapsToInt64() throws Exception {
            typeTest()
                    .column("NUMERIC(15, 0) NOT NULL",
                            PrimitiveType.Int64)
                        .value("999999999999999", 999999999999999L)
                        .value("-999999999999999", -999999999999999L)
                    .execute();
        }

        @Test
        public void highPrecisionDecimalMapsToDecimal() throws Exception {
            typeTest()
                    .column("NUMERIC(25, 0) NOT NULL",
                            DecimalType.of(35, 0))
                        .value("1234567890123456789012345",
                                new BigDecimal(
                                        "1234567890123456789012345"))
                    .execute();
        }

        @Test
        public void decimalAliasesMapToNumeric() throws Exception {
            typeTest()
                    .column("DECIMAL(10, 2) NOT NULL",
                            DecimalType.of(10, 2))
                        .value("123.45", new BigDecimal("123.45"))
                    .column("NUMBER(10, 2) NOT NULL",
                            DecimalType.of(10, 2))
                        .value("67.89", new BigDecimal("67.89"))
                    .column("MONEY NOT NULL",
                            DecimalType.of(18, 4))
                        .value("99.99", new BigDecimal("99.9900"))
                    .execute();
        }

        @Test
        public void defaultDecimalTypeWhenCustomDisabled()
                throws Exception {
            typeTest()
                    .withOptions(
                            opts -> opts.setAllowCustomDecimal(false))
                    .column("NUMERIC(10, 4) NOT NULL",
                            DecimalType.getDefault())
                        .value("123.4567",
                                new BigDecimal("123.4567"))
                    .execute();
        }

        @Test
        public void dateType() throws Exception {
            typeTest()
                    .column("DATE NOT NULL", PrimitiveType.Date32)
                        .value("'1970-01-01'",
                                LocalDate.of(1970, 1, 1))
                        .value("'2024-01-15'",
                                LocalDate.of(2024, 1, 15))
                    .column("DATE",
                            PrimitiveType.Date32.makeOptional())
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
        public void dateAsText() throws Exception {
            typeTest()
                    .withOptions(
                            opts -> opts.setDateConv(DateConv.STR))
                    .column("DATE NOT NULL", PrimitiveType.Text)
                        .value("'1970-01-01'", "1970/01/01")
                        .value("'2024-01-15'", "2024/01/15")
                    .execute();
        }

        @Test
        public void timestampMapsToTimestamp64() throws Exception {
            typeTest()
                    .column("TIMESTAMP NOT NULL",
                            PrimitiveType.Timestamp64)
                        .value("'2024-01-15 10:30:45'",
                                Instant.parse(
                                        "2024-01-15T10:30:45Z"))
                        .value("'2024-01-15 10:30:45.123456'",
                                Instant.parse(
                                    "2024-01-15T10:30:45.123456Z"))
                    .execute();
        }

        @Test
        public void timestampWithTimeZone() throws Exception {
            typeTest()
                    .column("TIMESTAMPTZ NOT NULL",
                            PrimitiveType.Timestamp64)
                        .value("'2024-01-15 10:30:45+00'::TIMESTAMPTZ",
                                Instant.parse(
                                        "2024-01-15T10:30:45Z"))
                        .value("'2024-06-20 15:00:00+03'::TIMESTAMPTZ",
                                Instant.parse(
                                        "2024-06-20T12:00:00Z"))
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
    }

    @Nested class TypeTestsRow extends VerticaTypeCases {
        @Override public boolean useArrow() { return false; }
    }

    @Nested class TypeTestsArrow extends VerticaTypeCases {
        @Override public boolean useArrow() { return true; }
    }

    abstract class VerticaTableCases
            extends AbstractYdbImporterTableTest {

        public abstract boolean useArrow();

        @Override
        public SourceDb sourceDb() {
            return new SourceDb(verticaContainer, SourceType.VERTICA, "public");
        }

        @Override
        protected String smallBlob(byte[] data) {
            return "HEX_TO_BINARY('" + hex(data) + "')";
        }

        @Override
        protected String bigBlob(String hexPair, int count) {
            byte[] data = filled(count, (byte) Integer.parseInt(hexPair, 16));
            return "HEX_TO_BINARY('" + hex(data) + "')";
        }

        /*
         * Vertica specific: ENABLED constraints become PK,
         * advisory (not enabled) fall back to synthetic key.
         */
        @Test
        public void primaryKeySelection() throws Exception {
            importTogether()
                    
                    .add(tableTest("public", "pk_enforced")
                            .setupSql(
                                "CREATE TABLE public.pk_enforced ("
                                + "  id INT NOT NULL,"
                                + "  name VARCHAR(50) NOT NULL,"
                                + "  CONSTRAINT pk_enf PRIMARY KEY (id) ENABLED"
                                + ");"
                                + "INSERT INTO public.pk_enforced VALUES (1,'a');"
                                + "INSERT INTO public.pk_enforced VALUES (2,'b')")
                            .cleanupSql("DROP TABLE IF EXISTS "
                                    + "public.pk_enforced CASCADE")
                            .expectPrimaryKey("id")
                            .expectRowCount(2)
                            .expectRowExists("id", 1L, "name", "a"))
                    .add(tableTest("public", "pk_advisory")
                            .setupSql(
                                "CREATE TABLE public.pk_advisory ("
                                + "  id INT NOT NULL,"
                                + "  name VARCHAR(50) NOT NULL,"
                                + "  PRIMARY KEY (id)"
                                + ");"
                                + "INSERT INTO public.pk_advisory VALUES (1,'a');"
                                + "INSERT INTO public.pk_advisory VALUES (2,'b')")
                            .cleanupSql("DROP TABLE IF EXISTS "
                                    + "public.pk_advisory CASCADE")
                            .expectSyntheticKey()
                            .expectRowCount(2)
                            .expectRowExists("id", 1L, "name", "a"))
                    .add(tableTest("public", "uq_enabled")
                            .setupSql(
                                "CREATE TABLE public.uq_enabled ("
                                + "  code VARCHAR(20) NOT NULL,"
                                + "  value INT NOT NULL,"
                                + "  CONSTRAINT uq_c UNIQUE (code) ENABLED"
                                + ");"
                                + "INSERT INTO public.uq_enabled VALUES ('A',10);"
                                + "INSERT INTO public.uq_enabled VALUES ('B',20)")
                            .cleanupSql("DROP TABLE IF EXISTS "
                                    + "public.uq_enabled CASCADE")
                            .expectPrimaryKey("code")
                            .expectRowCount(2)
                            .expectRowExists("code", "A", "value", 10L))
                    .add(tableTest("public", "uq_fewer_cols")
                            .setupSql(
                                "CREATE TABLE public.uq_fewer_cols ("
                                + "  a BIGINT NOT NULL,"
                                + "  b VARCHAR(10) NOT NULL,"
                                + "  c VARCHAR(10) NOT NULL,"
                                + "  CONSTRAINT uq_ab UNIQUE (a, b) ENABLED,"
                                + "  CONSTRAINT uq_c UNIQUE (c) ENABLED"
                                + ");"
                                + "INSERT INTO public.uq_fewer_cols VALUES (1,'x','p');"
                                + "INSERT INTO public.uq_fewer_cols VALUES (2,'y','q')")
                            .cleanupSql("DROP TABLE IF EXISTS "
                                    + "public.uq_fewer_cols CASCADE")
                            .expectPrimaryKey("c")
                            .expectRowCount(2)
                            .expectRowExists("c", "p"))
                    .add(tableTest("public", "uq_sorted_names")
                            .setupSql(
                                "CREATE TABLE public.uq_sorted_names ("
                                + "  x BIGINT NOT NULL,"
                                + "  a VARCHAR(10) NOT NULL,"
                                + "  b VARCHAR(10) NOT NULL,"
                                + "  z VARCHAR(10) NOT NULL,"
                                + "  CONSTRAINT uq_za UNIQUE (z, a) ENABLED,"
                                + "  CONSTRAINT uq_ab UNIQUE (a, b) ENABLED"
                                + ");"
                                + "INSERT INTO public.uq_sorted_names VALUES (1,'v1','v2','v3');"
                                + "INSERT INTO public.uq_sorted_names VALUES (2,'v4','v5','v6')")
                            .cleanupSql("DROP TABLE IF EXISTS "
                                    + "public.uq_sorted_names CASCADE")
                            .expectPrimaryKey("a", "b")
                            .expectRowCount(2)
                            .expectRowExists("a", "v1", "b", "v2"))
                    .run();
        }

        @Test
        public void partitionedImport() throws Exception {
            tableTest("public", "part_sales")
                    
                    .setupSql(
                        "CREATE TABLE public.part_sales ("
                        + "sale_id INT NOT NULL,"
                        + "region_id INT NOT NULL,"
                        + "amount INT NOT NULL,"
                        + "CONSTRAINT pk_ps PRIMARY KEY (sale_id) ENABLED"
                        + ") PARTITION BY region_id;"
                        + "INSERT INTO public.part_sales VALUES"
                        + " (1, 1, 100);"
                        + "INSERT INTO public.part_sales VALUES"
                        + " (2, 2, 200);"
                        + "INSERT INTO public.part_sales VALUES"
                        + " (3, 3, 300)")
                    .cleanupSql("DROP TABLE IF EXISTS "
                            + "public.part_sales CASCADE")
                    .expectPrimaryKey("sale_id")
                    .expectRowCount(3)
                    .expectRowExists("sale_id", 1L, "amount", 100L)
                    .expectRowExists("sale_id", 3L, "amount", 300L)
                    .run();
        }

        @Test
        public void emptyTableCreatesSchema() throws Exception {
            tableTest("public", "empty_tbl")
                    
                    .setupSql(
                        "CREATE TABLE public.empty_tbl ("
                        + "id INT NOT NULL,"
                        + "name VARCHAR(50) NOT NULL,"
                        + "CONSTRAINT pk_empty PRIMARY KEY (id) ENABLED"
                        + ")")
                    .cleanupSql("DROP TABLE IF EXISTS "
                            + "public.empty_tbl CASCADE")
                    .expectPrimaryKey("id")
                    .expectRowCount(0)
                    .run();
        }

        @Test
        public void customQueryTextImport() throws Exception {
            tableTest("public", "query_src")
                    
                    .setupSql(
                        "CREATE TABLE public.query_src ("
                        + "id INT NOT NULL,"
                        + "val VARCHAR(50) NOT NULL,"
                        + "CONSTRAINT pk_qs PRIMARY KEY (id) ENABLED"
                        + ");"
                        + "INSERT INTO public.query_src VALUES"
                        + " (1, 'a');"
                        + "INSERT INTO public.query_src VALUES"
                        + " (2, 'b');"
                        + "INSERT INTO public.query_src VALUES"
                        + " (3, 'c');"
                        + "INSERT INTO public.query_src VALUES"
                        + " (4, 'd')")
                    .cleanupSql("DROP TABLE IF EXISTS "
                            + "public.query_src CASCADE")
                    .queryText("SELECT id, val FROM public.query_src"
                            + " WHERE id <= 2")
                    .expectSyntheticKey()
                    .expectRowCount(2)
                    .expectRowExists("id", 1L, "val", "a")
                    .expectRowExists("id", 2L, "val", "b")
                    .run();
        }

        @Test
        public void syntheticKeyDistinguishesRows() throws Exception {
            tableTest("public", "synth_tbl")
                    
                    .setupSql(
                        "CREATE TABLE public.synth_tbl ("
                        + "seq INT NOT NULL,"
                        + "name VARCHAR(50) NOT NULL"
                        + ");"
                        + "INSERT INTO public.synth_tbl VALUES"
                        + " (1, 'same');"
                        + "INSERT INTO public.synth_tbl VALUES"
                        + " (2, 'same');"
                        + "INSERT INTO public.synth_tbl VALUES"
                        + " (3, 'same')")
                    .cleanupSql("DROP TABLE IF EXISTS "
                            + "public.synth_tbl CASCADE")
                    .expectSyntheticKey()
                    .expectRowCount(3)
                    .expectRowExists("seq", 1L, "name", "same")
                    .expectRowExists("seq", 2L, "name", "same")
                    .expectRowExists("seq", 3L, "name", "same")
                    .run();
        }

        @Test
        public void skipUnsupportedColumns() throws Exception {
            // UUID -> Types.OTHER, INTERVAL -> Types.OTHER
            tableTest("public", "skip_vertica")
                    
                    .withOptions(opts -> opts.setSkipUnknownTypes(true))
                    .setupSql(
                        "CREATE TABLE public.skip_vertica ("
                        + "id INT NOT NULL,"
                        + "name VARCHAR(50) NOT NULL,"
                        + "uid UUID,"
                        + "period INTERVAL DAY TO SECOND,"
                        + "CONSTRAINT pk_skip PRIMARY KEY (id) ENABLED"
                        + ");"
                        + "INSERT INTO public.skip_vertica VALUES"
                        + " (1, 'a', '01234567-89ab-cdef-0123-456789abcdef',"
                        + "  INTERVAL '1 12:00:00' DAY TO SECOND);"
                        + "INSERT INTO public.skip_vertica VALUES"
                        + " (2, 'b', '11234567-89ab-cdef-0123-456789abcdef',"
                        + "  INTERVAL '2 00:00:00' DAY TO SECOND)")
                    .cleanupSql("DROP TABLE IF EXISTS "
                            + "public.skip_vertica CASCADE")
                    .expectPrimaryKey("id")
                    .expectSkippedColumns("uid", "period")
                    .expectRowCount(2)
                    .expectRowExists("id", 1L, "name", "a")
                    .run();
        }

        @Test
        public void partitionedMultiBlobSynthKey() throws Exception {
            byte[] thumb1 = "thumb-1".getBytes();
            byte[] thumb2 = "thumb-2".getBytes();
            byte[] thumb3 = "thumb-3".getBytes();
            byte[] thumb4 = "thumb-4".getBytes();

            byte[] full1 = filled(524_288, (byte) 0x11);
            byte[] full2 = filled(524_288, (byte) 0x22);
            byte[] full3 = filled(524_288, (byte) 0x33);
            byte[] full4 = filled(524_288, (byte) 0x44);

            String insertSql =
                    "INSERT INTO public.mega_blob VALUES (?, ?, ?, ?)";
            tableTest("public", "mega_blob")

                    .setupSql(
                        "CREATE TABLE public.mega_blob ("
                        + "  region_id INT NOT NULL,"
                        + "  label VARCHAR(30),"
                        + "  thumbnail LONG VARBINARY,"
                        + "  fullsize LONG VARBINARY"
                        + ") PARTITION BY region_id")
                    .insertRow(insertSql, 1, "book-1", thumb1, full1)
                    .insertRow(insertSql, 1, "book-2", thumb2, full2)
                    .insertRow(insertSql, 1, "null-row", null, null)
                    .insertRow(insertSql, 2, "book-3", thumb3, full3)
                    .insertRow(insertSql, 2, "book-4", thumb4, full4)
                    .cleanupSql("DROP TABLE IF EXISTS "
                            + "public.mega_blob CASCADE")
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

            tableTest("public", "small_blob")

                    .setupSql(
                        "CREATE TABLE public.small_blob ("
                        + "  id INT NOT NULL,"
                        + "  name VARCHAR(50) NOT NULL,"
                        + "  data LONG VARBINARY,"
                        + "  CONSTRAINT pk_sb PRIMARY KEY (id) ENABLED"
                        + ");"
                        + "INSERT INTO public.small_blob VALUES (1, 'hello', "
                        + smallBlob("Hello".getBytes()) + ");"
                        + "INSERT INTO public.small_blob VALUES (2, 'null_blob', NULL);"
                        + "INSERT INTO public.small_blob VALUES (3, 'world', "
                        + smallBlob("World".getBytes()) + ")")
                    .cleanupSql("DROP TABLE IF EXISTS "
                            + "public.small_blob CASCADE")
                    .expectPrimaryKey("id")
                    .expectRowCount(3)
                    .expectBlobColumn("data")
                    .expectBlobBytes("data", "id", 1, "Hello".getBytes())
                    .expectBlobBytes("data", "id", 3, "World".getBytes())
                    .expectBlobBytes("data", "id", 2, null)
                    .run();
        }

        @Test
        public void clobContentImport() throws Exception {
            String bigText = repeat("Y", 524_288);
            String insertSql =
                    "INSERT INTO public.clob_docs VALUES (?, ?, ?)";

            tableTest("public", "clob_docs")
                    .setupSql(
                        "CREATE TABLE public.clob_docs ("
                        + "  id    INT NOT NULL,"
                        + "  short LONG VARCHAR,"
                        + "  large LONG VARCHAR,"
                        + "  CONSTRAINT pk_cd PRIMARY KEY (id) ENABLED"
                        + ")")
                    .insertRow(insertSql, 1, "YDB", bigText)
                    .insertRow(insertSql, 2, null, null)
                    .insertRow(insertSql, 3, "", "")
                    .cleanupSql("DROP TABLE IF EXISTS "
                            + "public.clob_docs CASCADE")
                    .clobColumns("short", "large")
                    .expectPrimaryKey("id")
                    .expectRowCount(3)
                    .expectClobColumn("short")
                    .expectClobColumn("large")
                    .expectClobContent("short", "id", 1, "YDB")
                    .expectClobContent("large", "id", 1, bigText)
                    .expectClobContent("short", "id", 2, null)
                    .expectClobContent("large", "id", 2, null)
                    .expectClobContent("short", "id", 3, "")
                    .expectClobContent("large", "id", 3, "")
                    .run();
        }
    }

    @Nested class TableTestsRow extends VerticaTableCases {
        @Override public boolean useArrow() { return false; }
    }

    @Nested class TableTestsArrow extends VerticaTableCases {
        @Override public boolean useArrow() { return true; }
    }
}
