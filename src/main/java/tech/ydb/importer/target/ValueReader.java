package tech.ydb.importer.target;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;

import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.Type;

/**
 * Adapter for converting values of different SQL types to YDB values.
 *
 * @author zinal
 */
public abstract class ValueReader {

    private static final ValueReader BOOL = new BoolReader(ValueWriter::writeBool);
    private static final ValueReader INT_BOOL = new IntReader((w, i, v) -> w.writeBool(i, v != 0));
    private static final ValueReader STR_BOOL = new StringReader((w, i, v) -> w.writeBool(i, str2bool(v)));

    private static final ValueReader TEXT = new StringReader(ValueWriter::writeText);
    private static final ValueReader BYTES = new BytesReader(ValueWriter::writeBytes);

    private static final ValueReader FLOAT = new FloatReader(ValueWriter::writeFloat);
    private static final ValueReader DOUBLE = new DoubleReader(ValueWriter::writeDouble);

    private static final ValueReader INT32 = new IntReader(ValueWriter::writeInt32);
    private static final ValueReader UINT32 = new LongReader(ValueWriter::writeUint32);
    private static final ValueReader INT64 = new LongReader(ValueWriter::writeInt64);
    private static final ValueReader UINT64 = new LongReader(ValueWriter::writeUint64);

    private static final ValueReader DATE = new DateReader((w, i, v) -> w.writeDate(i, v.toLocalDate()));
    private static final ValueReader DATE32 = new DateReader((w, i, v) -> w.writeDate32(i, v.toLocalDate()));
    private static final ValueReader DATE_INT32 = new DateReader((w, i, v) -> w.writeInt32(i, date2int(v)));
    private static final ValueReader DATE_UINT32 = new DateReader((w, i, v) -> w.writeUint32(i, date2int(v)));
    private static final ValueReader DATE_INT64 = new DateReader((w, i, v) -> w.writeInt64(i, date2int(v)));
    private static final ValueReader DATE_UINT64 = new DateReader((w, i, v) -> w.writeUint64(i, date2int(v)));
    private static final ValueReader DATE_STR = new DateReader((w, i, v) -> w.writeText(i, date2str(v)));

    private static final ValueReader TIME_INT32 = new TimeReader((w, i, v) -> w.writeInt32(i, time2int(v)));

    private static final ValueReader DATETIME = new TimestampReader((w, i, v) -> w.writeDatetime(i, v.toInstant()));
    private static final ValueReader TIMESTAMP = new TimestampReader((w, i, v) -> w.writeTimestamp(i, v.toInstant()));
    private static final ValueReader TS_DATE = new TimestampReader(
            (w, i, v) -> w.writeDate(i, v.toLocalDateTime().toLocalDate()));
    private static final ValueReader TS_DATE32 = new TimestampReader(
            (w, i, v) -> w.writeDate32(i, v.toLocalDateTime().toLocalDate()));
    private static final ValueReader DATETIME64 = new TimestampReader(
            (w, i, v) -> w.writeDatetime64(i, v.toInstant()));
    private static final ValueReader TIMESTAMP64 = new TimestampReader(
            (w, i, v) -> w.writeTimestamp64(i, v.toInstant()));

    private static final ValueReader TS_INT64 = new TimestampReader((w, i, v) -> w.writeInt64(i, v.getTime()));
    private static final ValueReader TS_UINT64 = new TimestampReader((w, i, v) -> w.writeUint64(i, v.getTime()));
    private static final ValueReader TS_STR = new TimestampReader((w, i, v) -> w.writeText(i, v.toString()));

    private static final ValueReader UUID_BINARY = new UuidReaderBinary();
    private static final ValueReader UUID_TEXT = new UuidReaderText();

    /**
     * Reads a single column from the source ResultSet, writes the converted value
     * through the provided ValueWriter and updates the synthetic key digest.
     *
     * @param rs Source result set
     * @param rsIdx Index of the source column
     * @param targetIdx Index of the target struct member
     * @param writer Destination for the converted value
     * @param synthKey Row synthetic key, or null when unused
     * @throws Exception
     */
    public abstract void read(ResultSet rs, int rsIdx, int targetIdx, ValueWriter writer, SynthKey synthKey)
            throws Exception;

    public void flush() {
        // Nothing
    }

    public static ValueReader getReader(Type ydbType, int sqlType) throws Exception {
        Type paramType = ydbType;
        while (Type.Kind.OPTIONAL.equals(paramType.getKind())) {
            paramType = paramType.unwrapOptional();
        }

        if (paramType.getKind() == Type.Kind.DECIMAL) {
            return new BigDecimalReader(ValueWriter::writeDecimal);
        }

        if (paramType.getKind() == Type.Kind.PRIMITIVE) {
            PrimitiveType type = (PrimitiveType) paramType;
            switch (type) {
                case Bool:
                    switch (sqlType) {
                        case java.sql.Types.SMALLINT:
                        case java.sql.Types.INTEGER:
                        case java.sql.Types.BIGINT:
                        case java.sql.Types.DECIMAL:
                        case java.sql.Types.NUMERIC:
                        case java.sql.Types.FLOAT:
                        case java.sql.Types.DOUBLE:
                            return INT_BOOL;
                        case java.sql.Types.CHAR:
                        case java.sql.Types.NCHAR:
                        case java.sql.Types.VARCHAR:
                        case java.sql.Types.NVARCHAR:
                            return STR_BOOL;
                        default:
                            return BOOL;
                    }
                case Date:
                    switch (sqlType) {
                        case java.sql.Types.TIMESTAMP:
                            return TS_DATE;
                        default:
                            return DATE;
                    }
                case Date32:
                    switch (sqlType) {
                        case java.sql.Types.TIMESTAMP:
                            return TS_DATE32;
                        default:
                            return DATE32;
                    }
                case Datetime:
                    return DATETIME;
                case Timestamp:
                    return TIMESTAMP;
                case Datetime64:
                    return DATETIME64;
                case Timestamp64:
                    return TIMESTAMP64;
                case Float:
                    return FLOAT;
                case Double:
                    return DOUBLE;
                case Int32:
                    switch (sqlType) {
                        case java.sql.Types.TIME:
                            return TIME_INT32;
                        case java.sql.Types.DATE:
                            return DATE_INT32;
                        default:
                            return INT32;
                    }
                case Int64:
                    switch (sqlType) {
                        case java.sql.Types.DATE:
                            return DATE_INT64;
                        case java.sql.Types.TIMESTAMP:
                            return TS_INT64;
                        default:
                            return INT64;
                    }
                case Uint32:
                    switch (sqlType) {
                        case java.sql.Types.DATE:
                            return DATE_UINT32;
                        default:
                            return UINT32;
                    }
                case Uint64:
                    switch (sqlType) {
                        case java.sql.Types.DATE:
                            return DATE_UINT64;
                        case java.sql.Types.TIMESTAMP:
                            return TS_UINT64;
                        default:
                            return UINT64;
                    }
                case Text:
                    switch (sqlType) {
                        case java.sql.Types.DATE:
                            return DATE_STR;
                        case java.sql.Types.TIMESTAMP:
                            return TS_STR;
                        default:
                            return TEXT;
                    }
                case Bytes: // SQL BINARY, VARBINARY
                    return BYTES;
                case Uuid:
                    switch (sqlType) {
                        case java.sql.Types.BINARY:
                        case java.sql.Types.VARBINARY:
                            return UUID_BINARY;
                        default:
                            return UUID_TEXT;
                    }
                default:
                    throw new IllegalArgumentException("unsupported type: " + paramType);
            }
        }
        throw new IllegalArgumentException("chooseMode(): " + paramType + " - unsupported kind");
    }

    @FunctionalInterface
    private interface BoolWriteOp {
        void write(ValueWriter w, int idx, boolean v);
    }

    @FunctionalInterface
    private interface IntWriteOp {
        void write(ValueWriter w, int idx, int v);
    }

    @FunctionalInterface
    private interface LongWriteOp {
        void write(ValueWriter w, int idx, long v);
    }

    @FunctionalInterface
    private interface FloatWriteOp {
        void write(ValueWriter w, int idx, float v);
    }

    @FunctionalInterface
    private interface DoubleWriteOp {
        void write(ValueWriter w, int idx, double v);
    }

    @FunctionalInterface
    private interface ObjWriteOp<T> {
        void write(ValueWriter w, int idx, T v);
    }

    private static class StringReader extends ValueReader {

        private final ObjWriteOp<String> op;

        StringReader(ObjWriteOp<String> op) {
            this.op = op;
        }

        @Override
        public void read(ResultSet rs, int rsIdx, int targetIdx, ValueWriter writer, SynthKey synthKey)
                throws Exception {
            String value = rs.getString(rsIdx);
            if (rs.wasNull()) {
                if (synthKey != null) {
                    synthKey.hashNull();
                }
                writer.writeNull(targetIdx);
                return;
            }
            if (synthKey != null) {
                synthKey.hashString(value);
            }
            op.write(writer, targetIdx, value);
        }
    }

    private static class BytesReader extends ValueReader {

        private final ObjWriteOp<byte[]> op;

        BytesReader(ObjWriteOp<byte[]> op) {
            this.op = op;
        }

        @Override
        public void read(ResultSet rs, int rsIdx, int targetIdx, ValueWriter writer, SynthKey synthKey)
                throws Exception {
            byte[] value = rs.getBytes(rsIdx);
            if (rs.wasNull()) {
                if (synthKey != null) {
                    synthKey.hashNull();
                }
                writer.writeNull(targetIdx);
                return;
            }
            if (synthKey != null) {
                synthKey.hashBytes(value);
            }
            op.write(writer, targetIdx, value);
        }
    }

    private static class IntReader extends ValueReader {

        private final IntWriteOp op;

        IntReader(IntWriteOp op) {
            this.op = op;
        }

        @Override
        public void read(ResultSet rs, int rsIdx, int targetIdx, ValueWriter writer, SynthKey synthKey)
                throws Exception {
            int value = rs.getInt(rsIdx);
            if (rs.wasNull()) {
                if (synthKey != null) {
                    synthKey.hashNull();
                }
                writer.writeNull(targetIdx);
                return;
            }
            if (synthKey != null) {
                synthKey.hashInt(value);
            }
            op.write(writer, targetIdx, value);
        }
    }

    private static class LongReader extends ValueReader {

        private final LongWriteOp op;

        LongReader(LongWriteOp op) {
            this.op = op;
        }

        @Override
        public void read(ResultSet rs, int rsIdx, int targetIdx, ValueWriter writer, SynthKey synthKey)
                throws Exception {
            long value = rs.getLong(rsIdx);
            if (rs.wasNull()) {
                if (synthKey != null) {
                    synthKey.hashNull();
                }
                writer.writeNull(targetIdx);
                return;
            }
            if (synthKey != null) {
                synthKey.hashLong(value);
            }
            op.write(writer, targetIdx, value);
        }
    }

    private static class FloatReader extends ValueReader {

        private final FloatWriteOp op;

        FloatReader(FloatWriteOp op) {
            this.op = op;
        }

        @Override
        public void read(ResultSet rs, int rsIdx, int targetIdx, ValueWriter writer, SynthKey synthKey)
                throws Exception {
            float value = rs.getFloat(rsIdx);
            if (rs.wasNull()) {
                if (synthKey != null) {
                    synthKey.hashNull();
                }
                writer.writeNull(targetIdx);
                return;
            }
            if (synthKey != null) {
                synthKey.hashFloat(value);
            }
            op.write(writer, targetIdx, value);
        }
    }

    private static class DoubleReader extends ValueReader {

        private final DoubleWriteOp op;

        DoubleReader(DoubleWriteOp op) {
            this.op = op;
        }

        @Override
        public void read(ResultSet rs, int rsIdx, int targetIdx, ValueWriter writer, SynthKey synthKey)
                throws Exception {
            double value = rs.getDouble(rsIdx);
            if (rs.wasNull()) {
                if (synthKey != null) {
                    synthKey.hashNull();
                }
                writer.writeNull(targetIdx);
                return;
            }
            if (synthKey != null) {
                synthKey.hashDouble(value);
            }
            op.write(writer, targetIdx, value);
        }
    }

    private static class BoolReader extends ValueReader {

        private final BoolWriteOp op;

        BoolReader(BoolWriteOp op) {
            this.op = op;
        }

        @Override
        public void read(ResultSet rs, int rsIdx, int targetIdx, ValueWriter writer, SynthKey synthKey)
                throws Exception {
            boolean value = rs.getBoolean(rsIdx);
            if (rs.wasNull()) {
                if (synthKey != null) {
                    synthKey.hashNull();
                }
                writer.writeNull(targetIdx);
                return;
            }
            if (synthKey != null) {
                synthKey.hashBool(value);
            }
            op.write(writer, targetIdx, value);
        }
    }

    private static class BigDecimalReader extends ValueReader {

        private final ObjWriteOp<BigDecimal> op;

        BigDecimalReader(ObjWriteOp<BigDecimal> op) {
            this.op = op;
        }

        @Override
        public void read(ResultSet rs, int rsIdx, int targetIdx, ValueWriter writer, SynthKey synthKey)
                throws Exception {
            BigDecimal value = rs.getBigDecimal(rsIdx);
            if (rs.wasNull()) {
                if (synthKey != null) {
                    synthKey.hashNull();
                }
                writer.writeNull(targetIdx);
                return;
            }
            if (synthKey != null) {
                synthKey.hashBigDecimal(value);
            }
            op.write(writer, targetIdx, value);
        }
    }

    private static class DateReader extends ValueReader {

        private final ObjWriteOp<Date> op;

        DateReader(ObjWriteOp<Date> op) {
            this.op = op;
        }

        @Override
        public void read(ResultSet rs, int rsIdx, int targetIdx, ValueWriter writer, SynthKey synthKey)
                throws Exception {
            Date value = rs.getDate(rsIdx);
            if (rs.wasNull()) {
                if (synthKey != null) {
                    synthKey.hashNull();
                }
                writer.writeNull(targetIdx);
                return;
            }
            if (synthKey != null) {
                synthKey.hashDate(value);
            }
            op.write(writer, targetIdx, value);
        }
    }

    private static class TimeReader extends ValueReader {

        private final ObjWriteOp<Time> op;

        TimeReader(ObjWriteOp<Time> op) {
            this.op = op;
        }

        @Override
        public void read(ResultSet rs, int rsIdx, int targetIdx, ValueWriter writer, SynthKey synthKey)
                throws Exception {
            Time value = rs.getTime(rsIdx);
            if (rs.wasNull()) {
                if (synthKey != null) {
                    synthKey.hashNull();
                }
                writer.writeNull(targetIdx);
                return;
            }
            if (synthKey != null) {
                synthKey.hashTime(value);
            }
            op.write(writer, targetIdx, value);
        }
    }

    private static class TimestampReader extends ValueReader {

        private final ObjWriteOp<Timestamp> op;

        TimestampReader(ObjWriteOp<Timestamp> op) {
            this.op = op;
        }

        @Override
        public void read(ResultSet rs, int rsIdx, int targetIdx, ValueWriter writer, SynthKey synthKey)
                throws Exception {
            Timestamp value = rs.getTimestamp(rsIdx);
            if (rs.wasNull()) {
                if (synthKey != null) {
                    synthKey.hashNull();
                }
                writer.writeNull(targetIdx);
                return;
            }
            if (synthKey != null) {
                synthKey.hashTimestamp(value);
            }
            op.write(writer, targetIdx, value);
        }
    }

    private static class UuidReaderText extends ValueReader {

        @Override
        public void read(ResultSet rs, int rsIdx, int targetIdx, ValueWriter writer, SynthKey synthKey)
                throws Exception {
            String value = rs.getString(rsIdx);
            if (rs.wasNull()) {
                if (synthKey != null) {
                    synthKey.hashNull();
                }
                writer.writeNull(targetIdx);
                return;
            }
            if (synthKey != null) {
                synthKey.hashBytes(value.getBytes(StandardCharsets.UTF_8));
            }
            writer.writeUuid(targetIdx, value);
        }
    }

    private static class UuidReaderBinary extends ValueReader {

        @Override
        public void read(ResultSet rs, int rsIdx, int targetIdx, ValueWriter writer, SynthKey synthKey)
                throws Exception {
            byte[] value = rs.getBytes(rsIdx);
            if (rs.wasNull()) {
                if (synthKey != null) {
                    synthKey.hashNull();
                }
                writer.writeNull(targetIdx);
                return;
            }
            if (synthKey != null) {
                synthKey.hashBytes(value);
            }
            ByteBuffer byteBuffer = ByteBuffer.wrap(value);
            long high = byteBuffer.getLong();
            long low = byteBuffer.getLong();
            writer.writeUuid(targetIdx, new UUID(high, low));
        }
    }

    private static boolean str2bool(String value) {
        if (value == null) {
            return false;
        }
        value = value.trim();
        if (value.length() == 0) {
            return false;
        }
        value = value.substring(0, 1);
        return !("N".equalsIgnoreCase(value)
                || "0".equalsIgnoreCase(value)
                || "F".equalsIgnoreCase(value)
                || "Н".equalsIgnoreCase(value)
                || "Л".equalsIgnoreCase(value));
    }

    private static int date2int(Date date) {
        LocalDate ld = date.toLocalDate();
        return (ld.getYear() * 10000) + (ld.getMonthValue() * 100) + ld.getDayOfMonth();
    }

    private static int time2int(Time time) {
        final LocalTime lt = time.toLocalTime();
        return 3600 * lt.getHour() + 60 * lt.getMinute() + lt.getSecond();
    }

    private static String date2str(Date date) {
        final LocalDate ld = date.toLocalDate();
        return String.format("%d/%02d/%02d", ld.getYear(), ld.getMonthValue(), ld.getDayOfMonth());
    }
}
