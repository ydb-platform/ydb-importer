package tech.ydb.importer.target;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.UUID;

import tech.ydb.table.values.DecimalType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.StructValue;
import tech.ydb.table.values.Value;
import tech.ydb.table.values.VoidValue;

/**
 * Row-based {@link ValueWriter}: accumulates column values into a {@code Value<?>[]}
 * array that is later assembled into a {@link StructValue}.
 */
public class RowValueWriter implements ValueWriter {

    private final Value<?>[] values;

    public RowValueWriter(int memberCount) {
        this.values = new Value<?>[memberCount];
        Arrays.fill(values, VoidValue.of());
    }

    public Value<?> getValue(int index) {
        return values[index];
    }

    public StructValue toStructValue(StructType type) {
        return type.newValueUnsafe(values);
    }

    @Override
    public void writeNull(int i) {
        values[i] = VoidValue.of();
    }

    @Override
    public void writeBool(int i, boolean v) {
        values[i] = PrimitiveValue.newBool(v);
    }

    @Override
    public void writeInt32(int i, int v) {
        values[i] = PrimitiveValue.newInt32(v);
    }

    @Override
    public void writeInt64(int i, long v) {
        values[i] = PrimitiveValue.newInt64(v);
    }

    @Override
    public void writeUint32(int i, long v) {
        values[i] = PrimitiveValue.newUint32(v);
    }

    @Override
    public void writeUint64(int i, long v) {
        values[i] = PrimitiveValue.newUint64(v);
    }

    @Override
    public void writeFloat(int i, float v) {
        values[i] = PrimitiveValue.newFloat(v);
    }

    @Override
    public void writeDouble(int i, double v) {
        values[i] = PrimitiveValue.newDouble(v);
    }

    @Override
    public void writeText(int i, String v) {
        values[i] = PrimitiveValue.newText(v);
    }

    @Override
    public void writeBytes(int i, byte[] v) {
        values[i] = PrimitiveValue.newBytes(v);
    }

    @Override
    public void writeDate(int i, LocalDate v) {
        values[i] = PrimitiveValue.newDate(v);
    }

    @Override
    public void writeDate32(int i, LocalDate v) {
        values[i] = PrimitiveValue.newDate32(v);
    }

    @Override
    public void writeDatetime(int i, Instant v) {
        values[i] = PrimitiveValue.newDatetime(v);
    }

    @Override
    public void writeTimestamp(int i, Instant v) {
        values[i] = PrimitiveValue.newTimestamp(v);
    }

    @Override
    public void writeDatetime64(int i, Instant v) {
        values[i] = PrimitiveValue.newDatetime64(v);
    }

    @Override
    public void writeTimestamp64(int i, Instant v) {
        values[i] = PrimitiveValue.newTimestamp64(v);
    }

    @Override
    public void writeDecimal(int i, DecimalType type, BigDecimal v) {
        values[i] = type.newValue(v);
    }

    @Override
    public void writeUuid(int i, UUID v) {
        values[i] = PrimitiveValue.newUuid(v);
    }

    @Override
    public void writeUuidFromString(int i, String v) {
        values[i] = PrimitiveValue.newUuid(v);
    }
}
