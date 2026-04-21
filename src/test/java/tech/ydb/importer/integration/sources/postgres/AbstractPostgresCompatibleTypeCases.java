package tech.ydb.importer.integration.sources.postgres;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;

import org.junit.jupiter.api.Test;

import tech.ydb.importer.config.TableOptions.DateConv;
import tech.ydb.importer.integration.typetest.AbstractYdbImporterTypeTest;
import tech.ydb.table.values.DecimalType;
import tech.ydb.table.values.PrimitiveType;

/**
 * Shared type-mapping tests for PostgreSQL-compatible databases
 * (PostgreSQL, Greenplum).
 */
public abstract class AbstractPostgresCompatibleTypeCases
        extends AbstractYdbImporterTypeTest {

    @Test
    public void int32Boundaries() throws Exception {
        typeTest()
                .column("INTEGER NOT NULL", PrimitiveType.Int32)
                    .value("0", 0)
                    .value("-1", -1)
                    .value("2147483647", Integer.MAX_VALUE)
                    .value("-2147483648", Integer.MIN_VALUE)
                .column("INTEGER", PrimitiveType.Int32.makeOptional())
                    .value("42", 42)
                    .value("NULL", null)
                .execute();
    }

    @Test
    public void int64Boundaries() throws Exception {
        typeTest()
                .column("BIGINT NOT NULL", PrimitiveType.Int64)
                    .value("0", 0L)
                    .value("9223372036854775807", Long.MAX_VALUE)
                    .value("-9223372036854775808", Long.MIN_VALUE)
                .execute();
    }

    @Test
    public void smallintMapsToInt32() throws Exception {
        typeTest()
                .column("SMALLINT NOT NULL", PrimitiveType.Int32)
                    .value("32767", 32767)
                    .value("-32768", -32768)
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
    public void doublePrecision() throws Exception {
        typeTest()
                .column("DOUBLE PRECISION NOT NULL",
                        PrimitiveType.Double)
                    .value("1.5", 1.5d)
                    .value("1e100", 1e100d)
                    .value("-0.25", -0.25d)
                .column("DOUBLE PRECISION",
                        PrimitiveType.Double.makeOptional())
                    .value("3.14", 3.14d)
                    .value("NULL", null)
                .execute();
    }

    @Test
    public void realMapsToFloat() throws Exception {
        // PostgreSQL REAL is 4-byte IEEE 754 single - exactly YDB Float.
        typeTest()
                .column("REAL NOT NULL", PrimitiveType.Float)
                    .value("1.5", 1.5f)
                    .value("-0.25", -0.25f)
                .column("REAL", PrimitiveType.Float.makeOptional())
                    .value("3.14", 3.14f)
                    .value("NULL", null)
                .execute();
    }

    @Test
    public void stringTypesMapToText() throws Exception {
        typeTest()
                .column("VARCHAR(100) NOT NULL", PrimitiveType.Text)
                    .value("'hello'", "hello")
                    .value("''", "")
                    .value("'кириллица'", "кириллица")
                    .value("'emoji \uD83D\uDE00'", "emoji \uD83D\uDE00")
                .column("TEXT NOT NULL", PrimitiveType.Text)
                    .value("'hello'", "hello")
                    .value("'long text value'", "long text value")
                    .value("'third'", "third")
                    .value("'fourth'", "fourth")
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
                .column("NUMERIC(5, 0) NOT NULL", PrimitiveType.Int32)
                    .value("12345", 12345)
                    .value("-99999", -99999)
                .execute();
    }

    @Test
    public void zeroScaleDecimalMapsToInt64() throws Exception {
        typeTest()
                .column("NUMERIC(15, 0) NOT NULL", PrimitiveType.Int64)
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
    public void numericUnbounded() throws Exception {
        typeTest()
                .column("NUMERIC NOT NULL", PrimitiveType.Double)
                    .value("42.5", 42.5d)
                    .value("-0.001", -0.001d)
                .execute();
    }

    @Test
    public void defaultDecimalTypeWhenCustomDisabled() throws Exception {
        typeTest()
                .withOptions(opts -> opts.setAllowCustomDecimal(false))
                .column("NUMERIC(10, 4) NOT NULL",
                        DecimalType.getDefault())
                    .value("123.4567", new BigDecimal("123.4567"))
                .execute();
    }

    @Test
    public void byteaMapsToBytes() throws Exception {
        typeTest()
                .column("BYTEA NOT NULL", PrimitiveType.Bytes)
                    .value("decode('DEADBEEF', 'hex')",
                            new byte[]{
                                (byte) 0xDE, (byte) 0xAD,
                                (byte) 0xBE, (byte) 0xEF})
                    .value("decode('00', 'hex')",
                            new byte[]{(byte) 0x00})
                .execute();
    }

    @Test
    public void dateType() throws Exception {
        typeTest()
                .column("DATE NOT NULL", PrimitiveType.Date32)
                    .value("DATE '1970-01-01'",
                            LocalDate.of(1970, 1, 1))
                    .value("DATE '2024-01-15'",
                            LocalDate.of(2024, 1, 15))
                    .value("DATE '1925-01-01'",
                            LocalDate.of(1925, 1, 1))
                .column("DATE", PrimitiveType.Date32.makeOptional())
                    .value("DATE '2024-01-15'",
                            LocalDate.of(2024, 1, 15))
                    .value("NULL", null)
                .execute();
    }

    @Test
    public void dateAsInt() throws Exception {
        typeTest()
                .withOptions(opts -> opts.setDateConv(DateConv.INT))
                .column("DATE NOT NULL", PrimitiveType.Int32)
                    .value("DATE '1970-01-01'", 19700101)
                    .value("DATE '2024-01-15'", 20240115)
                .execute();
    }

    @Test
    public void dateAsText() throws Exception {
        typeTest()
                .withOptions(opts -> opts.setDateConv(DateConv.STR))
                .column("DATE NOT NULL", PrimitiveType.Text)
                    .value("DATE '1970-01-01'", "1970/01/01")
                    .value("DATE '2024-01-15'", "2024/01/15")
                .execute();
    }

    @Test
    public void timestampMapsToTimestamp64() throws Exception {
        typeTest()
                .column("TIMESTAMP NOT NULL",
                        PrimitiveType.Timestamp64)
                    .value("TIMESTAMP '2024-01-15 10:30:45'",
                            Instant.parse("2024-01-15T10:30:45Z"))
                    .value("TIMESTAMP '2024-01-15 10:30:45.123456'",
                            Instant.parse(
                                    "2024-01-15T10:30:45.123456Z"))
                .execute();
    }

    @Test
    public void timestampZeroPrecisionMapsToDatetime64() throws Exception {
        // TIMESTAMP(0) scale=0 -> Datetime64 (seconds precision).
        typeTest()
                .column("TIMESTAMP(0) NOT NULL",
                        PrimitiveType.Datetime64)
                    .value("TIMESTAMP '2024-01-15 10:30:45'",
                            LocalDateTime.of(2024, 1, 15, 10, 30, 45))
                .execute();
    }

    @Test
    public void timestampWithTimeZone() throws Exception {
        /*
         * PG JDBC maps timestamptz to Types.TIMESTAMP, not
         * Types.TIMESTAMP_WITH_TIMEZONE.
         */
        typeTest()
                .column("TIMESTAMP WITH TIME ZONE NOT NULL",
                        PrimitiveType.Timestamp64)
                    .value("TIMESTAMPTZ '2024-01-15 10:30:45+00'",
                            Instant.parse("2024-01-15T10:30:45Z"))
                    .value("TIMESTAMPTZ '2024-06-20 15:00:00+03'",
                            Instant.parse("2024-06-20T12:00:00Z"))
                .execute();
    }

    @Test
    public void timeMapsToInt32() throws Exception {
        typeTest()
                .column("TIME NOT NULL", PrimitiveType.Int32)
                    .value("TIME '00:00:00'", 0)
                    .value("TIME '10:30:45'", 37845)
                    .value("TIME '23:59:59'", 86399)
                .execute();
    }
}
