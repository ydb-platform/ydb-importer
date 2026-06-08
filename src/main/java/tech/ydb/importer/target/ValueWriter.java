package tech.ydb.importer.target;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

import tech.ydb.table.values.DecimalType;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.Type;

/**
 * Receives column values produced by ValueReader during row construction.
 */
public interface ValueWriter {

    void writeNull(int idx);

    void writeBool(int idx, boolean v);

    void writeInt32(int idx, int v);

    void writeUint32(int idx, long v);

    void writeInt64(int idx, long v);

    void writeUint64(int idx, long v);

    void writeFloat(int idx, float v);

    void writeDouble(int idx, double v);

    void writeText(int idx, String v);

    void writeBytes(int idx, byte[] v);

    void writeUuid(int idx, String v);

    void writeUuid(int idx, UUID v);

    void writeDate(int idx, LocalDate v);

    void writeDate32(int idx, LocalDate v);

    void writeDatetime(int idx, Instant v);

    void writeDatetime64(int idx, Instant v);

    void writeTimestamp(int idx, Instant v);

    void writeTimestamp64(int idx, Instant v);

    void writeDecimal(int idx, BigDecimal v);

    static DecimalType[] precomputeDecimalTypes(StructType type) {
        DecimalType[] result = new DecimalType[type.getMembersCount()];
        for (int i = 0; i < result.length; i++) {
            Type t = type.getMemberType(i);
            if (t.getKind() == Type.Kind.OPTIONAL) {
                t = t.unwrapOptional();
            }
            if (t.getKind() == Type.Kind.DECIMAL) {
                result[i] = (DecimalType) t;
            }
        }
        return result;
    }
}
