package tech.ydb.importer.target;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

import tech.ydb.table.query.arrow.ApacheArrowWriter;
import tech.ydb.table.values.DecimalType;
import tech.ydb.table.values.StructType;

/**
 * Arrow-based {@link ValueWriter}: delegates writes to an {@link ApacheArrowWriter.Row}.
 * Column names are resolved from the {@link StructType} by index.
 */
public class ArrowValueWriter implements ValueWriter {

    private final ApacheArrowWriter.Row row;
    private final StructType type;

    public ArrowValueWriter(ApacheArrowWriter.Row row, StructType type) {
        this.row = row;
        this.type = type;
    }

    private String name(int i) {
        return type.getMemberName(i);
    }

    @Override
    public void writeNull(int i) {
        row.writeNull(name(i));
    }

    @Override
    public void writeBool(int i, boolean v) {
        row.writeBool(name(i), v);
    }

    @Override
    public void writeInt32(int i, int v) {
        row.writeInt32(name(i), v);
    }

    @Override
    public void writeInt64(int i, long v) {
        row.writeInt64(name(i), v);
    }

    @Override
    public void writeUint32(int i, long v) {
        row.writeUint32(name(i), v);
    }

    @Override
    public void writeUint64(int i, long v) {
        row.writeUint64(name(i), v);
    }

    @Override
    public void writeFloat(int i, float v) {
        row.writeFloat(name(i), v);
    }

    @Override
    public void writeDouble(int i, double v) {
        row.writeDouble(name(i), v);
    }

    @Override
    public void writeText(int i, String v) {
        row.writeText(name(i), v);
    }

    @Override
    public void writeBytes(int i, byte[] v) {
        row.writeBytes(name(i), v);
    }

    @Override
    public void writeDate(int i, LocalDate v) {
        row.writeDate(name(i), v);
    }

    @Override
    public void writeDate32(int i, LocalDate v) {
        row.writeDate32(name(i), v);
    }

    @Override
    public void writeDatetime(int i, Instant v) {
        row.writeDatetime(name(i), LocalDateTime.ofInstant(v, ZoneOffset.UTC));
    }

    @Override
    public void writeTimestamp(int i, Instant v) {
        row.writeTimestamp(name(i), v);
    }

    @Override
    public void writeDatetime64(int i, Instant v) {
        row.writeDatetime64(name(i), LocalDateTime.ofInstant(v, ZoneOffset.UTC));
    }

    @Override
    public void writeTimestamp64(int i, Instant v) {
        row.writeTimestamp64(name(i), v);
    }

    @Override
    public void writeDecimal(int i, DecimalType type, BigDecimal v) {
        row.writeDecimal(name(i), type.newValue(v));
    }

    @Override
    public void writeUuid(int i, UUID v) {
        row.writeUuid(name(i), v);
    }

    @Override
    public void writeUuidFromString(int i, String v) {
        row.writeUuid(name(i), UUID.fromString(v));
    }
}
