package tech.ydb.importer.integration.sources.clickhouse;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.TimeZone;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.testcontainers.clickhouse.ClickHouseContainer;

import tech.ydb.importer.config.SourceType;
import tech.ydb.importer.config.TableOptions.DateConv;
import tech.ydb.importer.integration.common.YdbImporterRunner;
import tech.ydb.importer.integration.common.YdbSchemaReader;
import tech.ydb.importer.integration.tabletest.AbstractYdbImporterTableTest;
import tech.ydb.importer.integration.typetest.AbstractYdbImporterTypeTest;
import tech.ydb.table.values.DecimalType;
import tech.ydb.table.values.PrimitiveType;

/** ClickHouse integration tests. */
public class ClickHouseYdbImporterTest {

    private static ClickHouseContainer chContainer;
    private static TimeZone originalTz;

    @BeforeAll
    static void startClickHouse() {
        originalTz = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        chContainer = ClickHouseTestContainer.create();
        chContainer.start();
    }

    @AfterAll
    static void stopClickHouse() {
        if (chContainer != null) {
            chContainer.stop();
            chContainer = null;
        }
        if (originalTz != null) {
            TimeZone.setDefault(originalTz);
        }
    }

    abstract class ClickHouseTypeCases extends AbstractYdbImporterTypeTest {

        public abstract boolean useArrow();

        @Override
        public SourceDb sourceDb() {
            return new SourceDb(chContainer, SourceType.CLICKHOUSE, "default");
        }

        @Override
        protected String createTableSuffix() {
            return " ENGINE = MergeTree() ORDER BY tuple()";
        }

        @Test
        public void int32Boundaries() throws Exception {
            typeTest()
                    .column("Int32", PrimitiveType.Int32)
                        .value("0", 0)
                        .value("-1", -1)
                        .value("2147483647", Integer.MAX_VALUE)
                        .value("-2147483648", Integer.MIN_VALUE)
                    .column("Nullable(Int32)", PrimitiveType.Int32.makeOptional())
                        .value("42", 42)
                        .value("NULL", null)
                    .execute();
        }

        @Test
        public void int64Boundaries() throws Exception {
            typeTest()
                    .column("Int64", PrimitiveType.Int64)
                        .value("0", 0L)
                        .value("9223372036854775807", Long.MAX_VALUE)
                        .value("-9223372036854775808", Long.MIN_VALUE)
                    .execute();
        }

        @Test
        public void int8AndInt16MapToInt32() throws Exception {
            typeTest()
                    .column("Int8", PrimitiveType.Int32)
                        .value("127", 127)
                        .value("-128", -128)
                    .column("Int16", PrimitiveType.Int32)
                        .value("32767", 32767)
                        .value("-32768", -32768)
                    .execute();
        }

        @Test
        public void uint8MapsToInt32() throws Exception {
            typeTest()
                    .column("UInt8", PrimitiveType.Int32)
                        .value("0", 0)
                        .value("255", 255)
                    .execute();
        }

        @Test
        public void uint16MapsToInt32() throws Exception {
            typeTest()
                    .column("UInt16", PrimitiveType.Int32)
                        .value("0", 0)
                        .value("65535", 65535)
                    .execute();
        }

        @Test
        public void uint32MapsToInt64() throws Exception {
            typeTest()
                    .column("UInt32", PrimitiveType.Int64)
                        .value("0", 0L)
                        .value("4294967295", 4294967295L)
                    .execute();
        }

        @Test
        public void uint64MapsToDecimal() throws Exception {
            typeTest()
                    .column("UInt64", DecimalType.of(35, 0))
                        .value("0", new BigDecimal("0"))
                        .value("18446744073709551615",
                               new BigDecimal("18446744073709551615"))
                    .execute();
        }

        @Test
        public void floatAndDouble() throws Exception {
            typeTest()
                    .column("Float32", PrimitiveType.Float)
                        .value("1.5", 1.5f)
                        .value("-0.25", -0.25f)
                    .column("Float64", PrimitiveType.Double)
                        .value("1.5", 1.5d)
                        .value("1e100", 1e100d)
                    .column("Nullable(Float64)",
                            PrimitiveType.Double.makeOptional())
                        .value("3.14", 3.14d)
                        .value("NULL", null)
                    .execute();
        }

        @Test
        public void floatNanAndInfinity() throws Exception {
            typeTest()
                    .column("Float32", PrimitiveType.Float)
                        .value("nan", Float.NaN)
                        .value("inf", Float.POSITIVE_INFINITY)
                        .value("-inf", Float.NEGATIVE_INFINITY)
                    .execute();
        }

        @Test
        public void stringTypesMapToText() throws Exception {
            typeTest()
                    .column("String", PrimitiveType.Text)
                        .value("'hello'", "hello")
                        .value("''", "")
                    .column("Nullable(String)",
                            PrimitiveType.Text.makeOptional())
                        .value("'hello'", "hello")
                        .value("NULL", null)
                    .execute();
        }

        @Test
        public void stringTypesPreserveUnicode() throws Exception {
            typeTest()
                    .column("String", PrimitiveType.Text)
                        .value("'кириллица'", "кириллица")
                        .value("'emoji \uD83D\uDE00'", "emoji \uD83D\uDE00")
                    .execute();
        }

        @Test
        public void enum8MapsToTextLabel() throws Exception {
            typeTest()
                    .column("Enum8('alpha'=1, 'beta'=2)", PrimitiveType.Text)
                        .value("'alpha'", "alpha")
                        .value("'beta'", "beta")
                    .execute();
        }

        @Test
        public void fixedStringMapsToText() throws Exception {
            typeTest()
                    .column("FixedString(10)", PrimitiveType.Text)
                        .value("'hello'", "hello\0\0\0\0\0")
                    .execute();
        }

        @Test
        public void lowCardinalityStringMapsToText() throws Exception {
            typeTest()
                    .column("LowCardinality(String)", PrimitiveType.Text)
                        .value("'hello'", "hello")
                        .value("'world'", "world")
                    .execute();
        }

        @Test
        public void booleanMaps() throws Exception {
            typeTest()
                    .column("Bool", PrimitiveType.Bool)
                        .value("true", true)
                        .value("false", false)
                    .execute();
        }

        @Test
        public void scaledDecimalMapsToDecimal() throws Exception {
            typeTest()
                    .column("Decimal(10, 4)", DecimalType.of(10, 4))
                        .value("123.4567", new BigDecimal("123.4567"))
                        .value("-999999.9999", new BigDecimal("-999999.9999"))
                        .value("0", new BigDecimal("0.0000"))
                    .column("Nullable(Decimal(10, 2))",
                            DecimalType.of(10, 2).makeOptional())
                        .value("99.99", new BigDecimal("99.99"))
                        .value("NULL", null)
                    .execute();
        }

        @Test
        public void zeroScaleDecimalMapsToInt32() throws Exception {
            typeTest()
                    .column("Decimal(5, 0)", PrimitiveType.Int32)
                        .value("12345", 12345)
                        .value("-99999", -99999)
                    .execute();
        }

        @Test
        public void zeroScaleDecimalMapsToInt64() throws Exception {
            typeTest()
                    .column("Decimal(15, 0)", PrimitiveType.Int64)
                        .value("999999999999999", 999999999999999L)
                        .value("-999999999999999", -999999999999999L)
                    .execute();
        }

        @Test
        public void highPrecisionDecimalMapsToDecimal() throws Exception {
            typeTest()
                    .column("Decimal(20, 0)", DecimalType.of(35, 0))
                        .value("12345678901234567890",
                               new BigDecimal("12345678901234567890"))
                    .execute();
        }

        @Test
        public void defaultDecimalTypeWhenCustomDisabled() throws Exception {
            typeTest()
                    .withOptions(opts -> opts.setAllowCustomDecimal(false))
                    .column("Decimal(10, 4)", DecimalType.getDefault())
                        .value("123.4567", new BigDecimal("123.4567"))
                    .execute();
        }

        @Test
        public void dateTypes() throws Exception {
            typeTest()
                    .column("Date", PrimitiveType.Date32)
                        .value("'1970-01-01'", LocalDate.of(1970, 1, 1))
                        .value("'2024-01-15'", LocalDate.of(2024, 1, 15))
                        .value("'2106-02-07'", LocalDate.of(2106, 2, 7))
                    .column("Date32", PrimitiveType.Date32)
                        .value("'1925-01-01'", LocalDate.of(1925, 1, 1))
                        .value("'1900-03-15'", LocalDate.of(1900, 3, 15))
                    .column("Nullable(Date)",
                            PrimitiveType.Date32.makeOptional())
                        .value("'2024-01-15'", LocalDate.of(2024, 1, 15))
                        .value("NULL", null)
                    .execute();
        }

        @Test
        public void dateAsInt() throws Exception {
            typeTest()
                    .withOptions(opts -> opts.setDateConv(DateConv.INT))
                    .column("Date", PrimitiveType.Int32)
                        .value("'1970-01-01'", 19700101)
                        .value("'2024-01-15'", 20240115)
                    .execute();
        }

        @Test
        public void dateAsText() throws Exception {
            typeTest()
                    .withOptions(opts -> opts.setDateConv(DateConv.STR))
                    .column("Date", PrimitiveType.Text)
                        .value("'1970-01-01'", "1970/01/01")
                        .value("'2024-01-15'", "2024/01/15")
                    .execute();
        }

        @Test
        public void datetimeMapsToDatetime64() throws Exception {
            typeTest()
                    .column("DateTime", PrimitiveType.Datetime64)
                        .value("'2024-01-15 10:30:45'",
                                java.time.LocalDateTime.of(2024, 1, 15, 10, 30, 45))
                    .execute();
        }

        @Test
        public void timestampMapsToTimestamp64() throws Exception {
            typeTest()
                    .column("DateTime64(3, 'UTC')", PrimitiveType.Timestamp64)
                        .value("toDateTime64('2024-01-15 10:30:45.123', 3, 'UTC')",
                               java.time.Instant.parse("2024-01-15T10:30:45.123Z"))
                    .column("DateTime64(6, 'UTC')", PrimitiveType.Timestamp64)
                        .value("toDateTime64('2024-01-15 10:30:45.123456', 6, 'UTC')",
                               java.time.Instant.parse("2024-01-15T10:30:45.123456Z"))
                    .execute();
        }

    }

    @Nested class TypeTestsRow extends ClickHouseTypeCases {
        @Override public boolean useArrow() { return false; }
    }

    @Nested class TypeTestsArrow extends ClickHouseTypeCases {
        @Override public boolean useArrow() { return true; }
    }

    abstract class ClickHouseTableCases extends AbstractYdbImporterTableTest {

        public abstract boolean useArrow();

        @Override
        public SourceDb sourceDb() {
            return new SourceDb(chContainer, SourceType.CLICKHOUSE, "default");
        }

        void cleanup(String... sourceQualifiedNames) {
            try (Connection con = openSourceConnection();
                 Statement st = con.createStatement()) {
                for (String name : sourceQualifiedNames) {
                    st.execute("DROP TABLE IF EXISTS " + name);
                }
            } catch (Exception ignored) { }
            try (YdbSchemaReader ydb = new YdbSchemaReader(
                    ydbContainer().getConnectionString())) {
                for (String name : sourceQualifiedNames) {
                    ydb.dropTable(YdbImporterRunner.DEFAULT_TARGET_PREFIX
                            + "." + name);
                }
            } catch (Exception ignored) { }
        }

        @Test
        public void compositeOrderByStillUsesSyntheticKey() throws Exception {
            tableTest("default", "composite_order")
                    
                    .columns("a Int32, b Int32, payload String")
                    .tableSuffix("ENGINE = MergeTree() ORDER BY (a, b)")
                    .values("(1, 10, 'x'), (2, 20, 'y'), (3, 30, 'z')")
                    .expectSyntheticKey()
                    .expectRowCount(3)
                    .expectRowExists("a", 1, "b", 10, "payload", "x")
                    .run();
        }

        @Test
        public void partitionedWithChunking() throws Exception {
            tableTest("default", "chunked")
                    
                    .fetchSize(2)
                    .columns("id Int32, region_id Int32, val String")
                    .tableSuffix("ENGINE = MergeTree()"
                            + " PARTITION BY intDiv(region_id, 25)"
                            + " ORDER BY id")
                    .values("(1,1,'a'),(2,2,'b'),(3,3,'c'),"
                            + "(4,25,'d'),(5,26,'e'),(6,27,'f'),"
                            + "(7,28,'g'),"
                            + "(8,50,'h'),(9,51,'i'),"
                            + "(10,75,'j'),(11,76,'k'),"
                            + "(12,77,'l'),(13,78,'m')")
                    .expectSyntheticKey()
                    .expectRowCount(13)
                    .expectRowExists("id", 1, "val", "a")
                    .expectRowExists("id", 13, "val", "m")
                    .run();
        }

        @Test
        public void skipUnsupportedColumns() throws Exception {
            tableTest("default", "skip_all")
                    
                    .withOptions(opts -> opts.setSkipUnknownTypes(true))
                    .columns("id Int32, name String,"
                            + " tags Array(String),"
                            + " props Map(String, Int32),"
                            + " pair Tuple(Int32, String),"
                            + " uid UUID, ip4 IPv4, ip6 IPv6")
                    .tableSuffix("ENGINE = MergeTree() ORDER BY id")
                    .values("(1,'a',['x'],{'k':1},(10,'x'),"
                            + "generateUUIDv4(),'127.0.0.1','::1'),"
                            + "(2,'b',['y'],{'k':2},(20,'y'),"
                            + "generateUUIDv4(),'10.0.0.1','fe80::1')")
                    .expectSyntheticKey()
                    .expectSkippedColumns("tags", "props", "pair",
                            "uid", "ip4", "ip6")
                    .expectRowCount(2)
                    .expectRowExists("id", 1, "name", "a")
                    .expectRowExists("id", 2, "name", "b")
                    .run();
        }

        @Test
        public void variousEngines() throws Exception {
            importTogether()
                    
                    .add(tableTest("default", "eng_rmt")
                            .columns("id Int32, val String, version Int32")
                            .tableSuffix("ENGINE = ReplacingMergeTree(version)"
                                    + " ORDER BY id")
                            .values("(1,'a',1),(2,'b',1)")
                            .expectSyntheticKey()
                            .expectRowCount(2)
                            .expectRowExists("id", 1, "val", "a")
                            .expectRowExists("id", 2, "val", "b"))
                    .add(tableTest("default", "eng_log")
                            .columns("id Int32, val String")
                            .tableSuffix("ENGINE = Log")
                            .values("(1,'x'),(2,'y')")
                            .expectSyntheticKey()
                            .expectRowCount(2)
                            .expectRowExists("id", 1, "val", "x")
                            .expectRowExists("id", 2, "val", "y"))
                    .add(tableTest("default", "eng_mem")
                            .columns("id Int32, val String")
                            .tableSuffix("ENGINE = Memory")
                            .values("(1,'p'),(2,'q')")
                            .expectSyntheticKey()
                            .expectRowCount(2)
                            .expectRowExists("id", 1, "val", "p")
                            .expectRowExists("id", 2, "val", "q"))
                    .run();
        }

        @Test
        public void emptyTableCreatesSchema() throws Exception {
            tableTest("default", "empty")
                    
                    .columns("id Int32, val String")
                    .tableSuffix("ENGINE = MergeTree() ORDER BY id")
                    .expectSyntheticKey()
                    .expectRowCount(0)
                    .run();
        }

        @Test
        public void customQueryTextImport() throws Exception {
            tableTest("default", "query_src")
                    
                    .columns("id Int32, val String")
                    .tableSuffix("ENGINE = MergeTree() ORDER BY id")
                    .values("(1,'a'),(2,'b'),(3,'c'),(4,'d')")
                    .queryText("SELECT id, val FROM default.query_src"
                            + " WHERE id <= 2")
                    .expectSyntheticKey()
                    .expectRowCount(2)
                    .expectRowExists("id", 1, "val", "a")
                    .expectRowExists("id", 2, "val", "b")
                    .run();
        }

        @Test
        public void syntheticKeyDistinguishesRows() throws Exception {
            tableTest("default", "synth_key_test")
                    
                    .columns("id Int32, name String")
                    .tableSuffix("ENGINE = MergeTree() ORDER BY id")
                    .values("(1,'same'),(2,'same'),(3,'same')")
                    .expectSyntheticKey()
                    .expectRowCount(3)
                    .expectRowExists("id", 1, "name", "same")
                    .expectRowExists("id", 2, "name", "same")
                    .expectRowExists("id", 3, "name", "same")
                    .run();
        }

        @Test
        public void multiTablePartitionedImport() throws Exception {
            importTogether()
                    
                    .add(tableTest("default", "mt_sales")
                            .columns("sale_id Int32, region_id Int32,"
                                    + " amount Int32")
                            .tableSuffix("ENGINE = MergeTree()"
                                    + " PARTITION BY intDiv(region_id, 50)"
                                    + " ORDER BY sale_id")
                            .values("(1,10,100),(2,60,200),(3,90,300)")
                            .expectSyntheticKey()
                            .expectRowCount(3)
                            .expectRowExists("sale_id", 1, "amount", 100)
                            .expectRowExists("sale_id", 3, "amount", 300))
                    .add(tableTest("default", "mt_events")
                            .columns("id Int32, ts Date")
                            .tableSuffix("ENGINE = MergeTree()"
                                    + " PARTITION BY toYYYYMM(ts)"
                                    + " ORDER BY id")
                            .values("(1,'2024-01-15'),(2,'2024-02-20')")
                            .expectSyntheticKey()
                            .expectRowCount(2)
                            .expectRowExists("id", 1)
                            .expectRowExists("id", 2))
                    .run();
        }

        @Test
        public void clobContentImport() throws Exception {
            String bigText = repeat("Y", 524_288);

            tableTest("default", "clob_docs")
                    .setupSql(
                        "CREATE TABLE default.clob_docs ("
                        + "  id    Int32,"
                        + "  short String,"
                        + "  large String"
                        + ") ENGINE = MergeTree() ORDER BY id;"
                        + "INSERT INTO default.clob_docs VALUES"
                        + " (1, 'YDB', '" + bigText + "'),"
                        + " (2, '', '')")
                    .cleanupSql("DROP TABLE IF EXISTS default.clob_docs")
                    .clobColumns("short", "large")
                    .expectSyntheticKey()
                    .expectRowCount(2)
                    .expectClobColumn("short")
                    .expectClobColumn("large")
                    .expectClobContent("short", "id", 1, "YDB")
                    .expectClobContent("large", "id", 1, bigText)
                    .expectClobContent("short", "id", 2, "")
                    .expectClobContent("large", "id", 2, "")
                    .run();
        }
    }

    @Nested class TableTestsRow extends ClickHouseTableCases {
        @Override public boolean useArrow() { return false; }
    }

    @Nested class TableTestsArrow extends ClickHouseTableCases {
        @Override public boolean useArrow() { return true; }
    }
}
