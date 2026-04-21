package tech.ydb.importer.integration.sources.mysql;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

import org.junit.jupiter.api.Test;

import tech.ydb.importer.config.TableOptions.DateConv;
import tech.ydb.importer.integration.typetest.AbstractYdbImporterTypeTest;
import tech.ydb.table.values.DecimalType;
import tech.ydb.table.values.PrimitiveType;

/**
 * Shared type-mapping tests for MySQL and MariaDB (InnoDB and ColumnStore).
 */
public abstract class AbstractMySqlCompatibleTypeCases
        extends AbstractYdbImporterTypeTest {


    @Override
    protected String createTableSuffix() {
        return " ENGINE=InnoDB";
    }

    @Test
    public void int32Boundaries() throws Exception {
        typeTest()
                .column("INT NOT NULL", PrimitiveType.Int32)
                    .value("0", 0)
                    .value("-1", -1)
                    .value("2147483647", Integer.MAX_VALUE)
                    .value("-2147483648", Integer.MIN_VALUE)
                .column("INT", PrimitiveType.Int32.makeOptional())
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
    public void mediumintMapsToInt32() throws Exception {
        typeTest()
                .column("MEDIUMINT NOT NULL", PrimitiveType.Int32)
                    .value("8388607", 8388607)
                    .value("-8388608", -8388608)
                .execute();
    }

    @Test
    public void tinyintMapsToInt32() throws Exception {
        typeTest()
                .column("TINYINT NOT NULL", PrimitiveType.Int32)
                    .value("127", 127)
                    .value("-128", -128)
                .execute();
    }

    @Test
    public void booleanMaps() throws Exception {
        // BOOLEAN = TINYINT(1). Drivers map it to BIT with tinyInt1isBit=true.
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
                .column("DOUBLE NOT NULL", PrimitiveType.Double)
                    .value("1.5", 1.5d)
                    .value("-0.25", -0.25d)
                .column("DOUBLE", PrimitiveType.Double.makeOptional())
                    .value("3.14", 3.14d)
                    .value("NULL", null)
                .execute();
    }

    @Test
    public void floatMapsToFloat() throws Exception {
        typeTest()
                .column("FLOAT NOT NULL", PrimitiveType.Float)
                    .value("1.5", 1.5f)
                    .value("-0.25", -0.25f)
                .execute();
    }

    @Test
    public void realMapsToDouble() throws Exception {
        // MySQL REAL is 8-byte synonym for DOUBLE. Driver reports Types.DOUBLE.
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
                .column("TEXT NOT NULL", PrimitiveType.Text)
                    .value("'hello'", "hello")
                    .value("'text value'", "text value")
                    .value("'third'", "third")
                .column("VARCHAR(100)",
                        PrimitiveType.Text.makeOptional())
                    .value("'hello'", "hello")
                    .value("NULL", null)
                .execute();
    }

    @Test
    public void charTrimsTrailingSpaces() throws Exception {
        // MySQL/MariaDB trim trailing spaces from CHAR on retrieval
        typeTest()
                .column("CHAR(10) NOT NULL", PrimitiveType.Text)
                    .value("'hello'", "hello")
                .execute();
    }

    @Test
    public void jsonAndEnumMapToText() throws Exception {
        // JSON and ENUM both map to Text through LONGVARCHAR / CHAR.
        typeTest()
                .column("JSON NOT NULL", PrimitiveType.Text)
                    .value("'{\"key\": \"value\"}'",
                            "{\"key\": \"value\"}")
                .column("ENUM('alpha','beta','gamma') NOT NULL",
                        PrimitiveType.Text)
                    .value("'alpha'", "alpha")
                .execute();
    }

    @Test
    public void binaryAndVarbinary() throws Exception {
        typeTest()
                .column("VARBINARY(100) NOT NULL", PrimitiveType.Bytes)
                    .value("X'DEADBEEF'",
                            new byte[]{(byte) 0xDE, (byte) 0xAD,
                                       (byte) 0xBE, (byte) 0xEF})
                    .value("X'00'",
                            new byte[]{(byte) 0x00})
                .execute();
    }

    @Test
    public void scaledDecimalMapsToDecimal() throws Exception {
        typeTest()
                .column("DECIMAL(10, 4) NOT NULL",
                        DecimalType.of(10, 4))
                    .value("123.4567", new BigDecimal("123.4567"))
                    .value("-999999.9999",
                            new BigDecimal("-999999.9999"))
                    .value("0", new BigDecimal("0.0000"))
                .column("DECIMAL(10, 2)",
                        DecimalType.of(10, 2).makeOptional())
                    .value("99.99", new BigDecimal("99.99"))
                    .value("NULL", null)
                .execute();
    }

    @Test
    public void zeroScaleDecimalMapsToInt32() throws Exception {
        typeTest()
                .column("DECIMAL(5, 0) NOT NULL", PrimitiveType.Int32)
                    .value("12345", 12345)
                    .value("-99999", -99999)
                .execute();
    }

    @Test
    public void zeroScaleDecimalMapsToInt64() throws Exception {
        typeTest()
                .column("DECIMAL(15, 0) NOT NULL", PrimitiveType.Int64)
                    .value("999999999999999", 999999999999999L)
                    .value("-999999999999999", -999999999999999L)
                .execute();
    }

    @Test
    public void highPrecisionDecimalMapsToDecimal() throws Exception {
        typeTest()
                .column("DECIMAL(25, 0) NOT NULL",
                        DecimalType.of(35, 0))
                    .value("1234567890123456789012345",
                            new BigDecimal(
                                    "1234567890123456789012345"))
                .execute();
    }

    @Test
    public void defaultDecimalTypeWhenCustomDisabled() throws Exception {
        typeTest()
                .withOptions(opts -> opts.setAllowCustomDecimal(false))
                .column("DECIMAL(10, 4) NOT NULL",
                        DecimalType.getDefault())
                    .value("123.4567", new BigDecimal("123.4567"))
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
                .column("DATE", PrimitiveType.Date32.makeOptional())
                    .value("'2024-01-15'",
                            LocalDate.of(2024, 1, 15))
                    .value("NULL", null)
                .execute();
    }

    @Test
    public void dateAsInt() throws Exception {
        typeTest()
                .withOptions(opts -> opts.setDateConv(DateConv.INT))
                .column("DATE NOT NULL", PrimitiveType.Int32)
                    .value("'1970-01-01'", 19700101)
                    .value("'2024-01-15'", 20240115)
                .execute();
    }

    @Test
    public void dateAsText() throws Exception {
        typeTest()
                .withOptions(opts -> opts.setDateConv(DateConv.STR))
                .column("DATE NOT NULL", PrimitiveType.Text)
                    .value("'1970-01-01'", "1970/01/01")
                    .value("'2024-01-15'", "2024/01/15")
                .execute();
    }

    @Test
    public void datetimeMapsToDatetime64() throws Exception {
        // DATETIME without precision -> scale=0 -> Datetime64.
        typeTest()
                .column("DATETIME NOT NULL", PrimitiveType.Datetime64)
                    .value("'2024-01-15 10:30:45'",
                            LocalDateTime.of(2024, 1, 15, 10, 30, 45))
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
