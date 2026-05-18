package tech.ydb.importer.target;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

import tech.ydb.table.values.DecimalType;
import tech.ydb.table.values.ListValue;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.Value;
import tech.ydb.table.values.VoidValue;

/**
 * ValueWriter that stores produced values into a Value array.
 */
public class RowValueWriter implements ValueWriter {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(RowValueWriter.class);

    private final DecimalType[] decimalTypes;
    private Value<?>[] values;

    public RowValueWriter(StructType type) {
        this.decimalTypes = ValueWriter.precomputeDecimalTypes(type);
    }

    public void setValues(Value<?>[] values) {
        this.values = values;
    }

    @Override
    public void writeNull(int idx) {
        values[idx] = VoidValue.of();
    }

    @Override
    public void writeBool(int idx, boolean v) {
        values[idx] = PrimitiveValue.newBool(v);
    }

    @Override
    public void writeInt32(int idx, int v) {
        values[idx] = PrimitiveValue.newInt32(v);
    }

    @Override
    public void writeUint32(int idx, long v) {
        values[idx] = PrimitiveValue.newUint32(v);
    }

    @Override
    public void writeInt64(int idx, long v) {
        values[idx] = PrimitiveValue.newInt64(v);
    }

    @Override
    public void writeUint64(int idx, long v) {
        values[idx] = PrimitiveValue.newUint64(v);
    }

    @Override
    public void writeFloat(int idx, float v) {
        values[idx] = PrimitiveValue.newFloat(v);
    }

    @Override
    public void writeDouble(int idx, double v) {
        values[idx] = PrimitiveValue.newDouble(v);
    }

    @Override
    public void writeText(int idx, String v) {
        values[idx] = PrimitiveValue.newText(v);
    }

    @Override
    public void writeBytes(int idx, byte[] v) {
        values[idx] = PrimitiveValue.newBytes(v);
    }

    @Override
    public void writeUuid(int idx, String v) {
        values[idx] = PrimitiveValue.newUuid(v);
    }

    @Override
    public void writeUuid(int idx, UUID v) {
        values[idx] = PrimitiveValue.newUuid(v);
    }

    @Override
    public void writeDate(int idx, LocalDate v) {
        values[idx] = PrimitiveValue.newDate(v);
    }

    @Override
    public void writeDate32(int idx, LocalDate v) {
        values[idx] = PrimitiveValue.newDate32(v);
    }

    @Override
    public void writeDatetime(int idx, Instant v) {
        values[idx] = PrimitiveValue.newDatetime(v);
    }

    @Override
    public void writeDatetime64(int idx, Instant v) {
        values[idx] = PrimitiveValue.newDatetime64(v);
    }

    @Override
    public void writeTimestamp(int idx, Instant v) {
        values[idx] = PrimitiveValue.newTimestamp(v);
    }

    @Override
    public void writeTimestamp64(int idx, Instant v) {
        values[idx] = PrimitiveValue.newTimestamp64(v);
    }

    @Override
    public void writeDecimal(int idx, BigDecimal v) {
        values[idx] = decimalTypes[idx].newValue(v);
    }

    public static void logValues(ListValue values) {
        int size = values.size();
        LOG.debug("********************************");
        LOG.debug("Problematic data block dump START, size is {}", size);
        for (int i = 0; i < size; ++i) {
            LOG.debug("{} {}", i, values.get(i));
        }
        LOG.debug("Problematic data block dump FINISH");
        LOG.debug("********************************");
    }
}
