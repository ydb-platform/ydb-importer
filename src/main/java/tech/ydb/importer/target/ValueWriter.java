package tech.ydb.importer.target;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

import tech.ydb.table.values.DecimalType;

/**
 * Abstraction over the output format for column values.
 * Two implementations: {@link RowValueWriter} for row-based bulk upsert
 * and {@link ArrowValueWriter} for Apache Arrow columnar bulk upsert.
 */
public interface ValueWriter {

    void writeNull(int index);

    void writeBool(int index, boolean val);

    void writeInt32(int index, int val);

    void writeInt64(int index, long val);

    void writeUint32(int index, long val);

    void writeUint64(int index, long val);

    void writeFloat(int index, float val);

    void writeDouble(int index, double val);

    void writeText(int index, String val);

    void writeBytes(int index, byte[] val);

    void writeDate(int index, LocalDate val);

    void writeDate32(int index, LocalDate val);

    void writeDatetime(int index, Instant val);

    void writeTimestamp(int index, Instant val);

    void writeDatetime64(int index, Instant val);

    void writeTimestamp64(int index, Instant val);

    void writeDecimal(int index, DecimalType type, BigDecimal val);

    void writeUuid(int index, UUID val);

    void writeUuidFromString(int index, String val);
}
