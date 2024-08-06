package tech.ydb.importer.target;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.function.Function;

import tech.ydb.table.values.DecimalType;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;
import tech.ydb.table.values.VoidValue;

/**
 *
 * @author zinal
 */
public abstract class ValueReader {
    private static final ValueReader BIG_DECIMAL = new BigDecimalReader(DecimalType.getDefault()::newValue);

    private static final ValueReader BOOL = new BoolReader(PrimitiveValue::newBool);
    private static final ValueReader INT_BOOL = new IntReader(i -> PrimitiveValue.newBool(i != 0));
    private static final ValueReader STR_BOOL = new StringReader(s -> PrimitiveValue.newBool(str2bool(s)));

    private static final ValueReader TEXT = new StringReader(PrimitiveValue::newText);
    private static final ValueReader BYTES = new BytesReader(PrimitiveValue::newBytes);

    private static final ValueReader FLOAT = new FloatReader(PrimitiveValue::newFloat);
    private static final ValueReader DOUBLE = new DoubleReader(PrimitiveValue::newDouble);

    private static final ValueReader INT32 = new IntReader(PrimitiveValue::newInt32);
    private static final ValueReader UINT32 = new LongReader(PrimitiveValue::newUint32);
    private static final ValueReader INT64 = new LongReader(PrimitiveValue::newInt64);
    private static final ValueReader UINT64 = new LongReader(PrimitiveValue::newUint64);

    private static final ValueReader DATE = new DateReader(date -> PrimitiveValue.newDate(date.toLocalDate()));
    private static final ValueReader DATE_INT32 = new DateReader(date -> PrimitiveValue.newInt32(date2int(date)));
    private static final ValueReader DATE_UINT32 = new DateReader(date -> PrimitiveValue.newUint32(date2int(date)));
    private static final ValueReader DATE_INT64 = new DateReader(date -> PrimitiveValue.newInt64(date2int(date)));
    private static final ValueReader DATE_UINT64 = new DateReader(date -> PrimitiveValue.newUint64(date2int(date)));
    private static final ValueReader DATE_STR = new DateReader(date -> PrimitiveValue.newText(date2str(date)));

    private static final ValueReader TIME_INT32 = new TimeReader(time -> PrimitiveValue.newInt32(time2int(time)));

    private static final ValueReader DATETIME = new TimestampReader(ts -> PrimitiveValue.newDatetime(ts.toInstant()));
    private static final ValueReader TIMESTAMP = new TimestampReader(ts -> PrimitiveValue.newTimestamp(ts.toInstant()));
    private static final ValueReader TS_DATE = new TimestampReader(
            ts -> PrimitiveValue.newDate(ts.toLocalDateTime().toLocalDate())
    );
    private static final ValueReader TS_INT64 = new TimestampReader(ts -> PrimitiveValue.newInt64(ts.getTime()));
    private static final ValueReader TS_UINT64 = new TimestampReader(ts -> PrimitiveValue.newUint64(ts.getTime()));
    private static final ValueReader TS_STR = new TimestampReader(ts -> PrimitiveValue.newText(ts.toString()));

    /**
     * Converts a single source ResultSet column value to YDB format.
     *
     * @param synthKey synthetic key of row
     * @param rs Input source result set
     * @param index Index of column
     * @return YDB-formatted value
     * @throws Exception
     */
    public abstract Value<?> readValue(SynthKey synthKey, ResultSet rs, int index) throws Exception;

    public static ValueReader getReader(Type ydbType, int sqlType) throws Exception {
        Type paramType = ydbType;
        while (Type.Kind.OPTIONAL.equals(paramType.getKind())) {
            paramType = paramType.unwrapOptional();
        }

        if (paramType.getKind() == Type.Kind.DECIMAL) {
            return BIG_DECIMAL;
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
                case Datetime:
                    return DATETIME;
                case Timestamp:
                    return TIMESTAMP;
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
                default:
                    throw new IllegalArgumentException("unsupported type: " + paramType);
            }
        }
        throw new IllegalArgumentException("chooseMode(): " + paramType + " - unsupported kind");
    }

    private static class StringReader extends ValueReader {
        private final Function<String, Value<?>> func;

        StringReader(Function<String, Value<?>> func) {
            this.func = func;
        }

        @Override
        public Value<?> readValue(SynthKey synthKey, ResultSet rs, int index) throws Exception {
            String value = rs.getString(index);
            if (rs.wasNull()) {
                if (synthKey != null) {
                    synthKey.updateSeparator();
                }
                return VoidValue.of();
            }

            if (synthKey != null) {
                synthKey.update(value.getBytes());
                synthKey.updateSeparator();
            }

            return func.apply(value);
        }
    }

    private static class BytesReader extends ValueReader {
        private final Function<byte[], Value<?>> func;

        BytesReader(Function<byte[], Value<?>> func) {
            this.func = func;
        }

        @Override
        public Value<?> readValue(SynthKey synthKey, ResultSet rs, int index) throws Exception {
            byte[] value = rs.getBytes(index);
            if (rs.wasNull()) {
                if (synthKey != null) {
                    synthKey.updateSeparator();
                }
                return VoidValue.of();
            }

            if (synthKey != null) {
                synthKey.update(value);
                synthKey.updateSeparator();
            }

            return func.apply(value);
        }
    }

    private static class IntReader extends ValueReader {
        private final Function<Integer, Value<?>> func;

        IntReader(Function<Integer, Value<?>> func) {
            this.func = func;
        }

        @Override
        public Value<?> readValue(SynthKey synthKey, ResultSet rs, int index) throws Exception {
            int value = rs.getInt(index);
            if (rs.wasNull()) {
                if (synthKey != null) {
                    synthKey.updateSeparator();
                }
                return VoidValue.of();
            }

            if (synthKey != null) {
                ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
                buffer.putInt(value);
                synthKey.update(buffer);
                synthKey.updateSeparator();
            }

            return func.apply(value);
        }
    }

    private static class LongReader extends ValueReader {
        private final Function<Long, Value<?>> func;

        LongReader(Function<Long, Value<?>> func) {
            this.func = func;
        }

        @Override
        public Value<?> readValue(SynthKey synthKey, ResultSet rs, int index) throws Exception {
            long value = rs.getLong(index);
            if (rs.wasNull()) {
                if (synthKey != null) {
                    synthKey.updateSeparator();
                }
                return VoidValue.of();
            }

            if (synthKey != null) {
                ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
                buffer.putLong(value);
                synthKey.update(buffer);
                synthKey.updateSeparator();
            }

            return func.apply(value);
        }
    }

    private static class FloatReader extends ValueReader {
        private final Function<Float, Value<?>> func;

        FloatReader(Function<Float, Value<?>> func) {
            this.func = func;
        }

        @Override
        public Value<?> readValue(SynthKey synthKey, ResultSet rs, int index) throws Exception {
            float value = rs.getFloat(index);
            if (rs.wasNull()) {
                if (synthKey != null) {
                    synthKey.updateSeparator();
                }
                return VoidValue.of();
            }

            if (synthKey != null) {
                ByteBuffer buffer = ByteBuffer.allocate(Float.BYTES);
                buffer.putFloat(value);
                synthKey.update(buffer);
                synthKey.updateSeparator();
            }

            return func.apply(value);
        }
    }

    private static class DoubleReader extends ValueReader {
        private final Function<Double, Value<?>> func;

        DoubleReader(Function<Double, Value<?>> func) {
            this.func = func;
        }

        @Override
        public Value<?> readValue(SynthKey synthKey, ResultSet rs, int index) throws Exception {
            double value = rs.getDouble(index);
            if (rs.wasNull()) {
                if (synthKey != null) {
                    synthKey.updateSeparator();
                }
                return VoidValue.of();
            }

            if (synthKey != null) {
                ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES);
                buffer.putDouble(value);
                synthKey.update(buffer);
                synthKey.updateSeparator();
            }

            return func.apply(value);
        }
    }
    private static class BoolReader extends ValueReader {
        private final Function<Boolean, Value<?>> func;

        BoolReader(Function<Boolean, Value<?>> func) {
            this.func = func;
        }

        @Override
        public Value<?> readValue(SynthKey synthKey, ResultSet rs, int index) throws Exception {
            boolean value = rs.getBoolean(index);
            if (rs.wasNull()) {
                if (synthKey != null) {
                    synthKey.updateSeparator();
                }
                return VoidValue.of();
            }

            if (synthKey != null) {
                ByteBuffer buffer = ByteBuffer.allocate(1);
                buffer.put((byte) (value ? 1 : 0));
                synthKey.update(buffer);
                synthKey.updateSeparator();
            }

            return func.apply(value);
        }
    }

    private static class BigDecimalReader extends ValueReader {
        private final Function<BigDecimal, Value<?>> func;

        BigDecimalReader(Function<BigDecimal, Value<?>> func) {
            this.func = func;
        }

        @Override
        public Value<?> readValue(SynthKey synthKey, ResultSet rs, int index) throws Exception {
            BigDecimal value = rs.getBigDecimal(index);
            if (rs.wasNull()) {
                if (synthKey != null) {
                    synthKey.updateSeparator();
                }
                return VoidValue.of();
            }

            if (synthKey != null) {
                byte[] bytes = value.toBigInteger().toByteArray();
                ByteBuffer buffer = ByteBuffer.allocate(bytes.length + Integer.BYTES);
                buffer.putInt(value.scale());
                buffer.put(bytes);
                synthKey.update(buffer);
                synthKey.updateSeparator();
            }

            return func.apply(value);
        }
    }

    private static class DateReader extends ValueReader {
        private final Function<Date, Value<?>> func;

        DateReader(Function<Date, Value<?>> func) {
            this.func = func;
        }

        @Override
        public Value<?> readValue(SynthKey synthKey, ResultSet rs, int index) throws Exception {
            Date value = rs.getDate(index);
            if (rs.wasNull()) {
                if (synthKey != null) {
                    synthKey.updateSeparator();
                }
                return VoidValue.of();
            }

            if (synthKey != null) {
                ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
                buffer.putLong(value.getTime());
                synthKey.update(buffer);
                synthKey.updateSeparator();
            }

            return func.apply(value);
        }
    }

    private static class TimeReader extends ValueReader {
        private final Function<Time, Value<?>> func;

        TimeReader(Function<Time, Value<?>> func) {
            this.func = func;
        }

        @Override
        public Value<?> readValue(SynthKey synthKey, ResultSet rs, int index) throws Exception {
            Time value = rs.getTime(index);
            if (rs.wasNull()) {
                if (synthKey != null) {
                    synthKey.updateSeparator();
                }
                return VoidValue.of();
            }

            if (synthKey != null) {
                LocalTime time = value.toLocalTime();
                ByteBuffer buffer = ByteBuffer.allocate(4 * Integer.BYTES);
                buffer.putInt(time.getHour());
                buffer.putInt(time.getMinute());
                buffer.putInt(time.getSecond());
                buffer.putInt(time.getNano());
                synthKey.update(buffer);
                synthKey.updateSeparator();
            }

            return func.apply(value);
        }
    }

    private static class TimestampReader extends ValueReader {
        private final Function<Timestamp, Value<?>> func;

        TimestampReader(Function<Timestamp, Value<?>> func) {
            this.func = func;
        }

        @Override
        public Value<?> readValue(SynthKey synthKey, ResultSet rs, int index) throws Exception {
            Timestamp value = rs.getTimestamp(index);
            if (rs.wasNull()) {
                if (synthKey != null) {
                    synthKey.updateSeparator();
                }
                return VoidValue.of();
            }

            if (synthKey != null) {
                ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
                buffer.putLong(value.getTime());
                buffer.putInt(value.getNanos());
                synthKey.update(buffer);
                synthKey.updateSeparator();
            }

            return func.apply(value);
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

    private static int date2int(java.sql.Date date) {
        LocalDate ld = date.toLocalDate();
        return (ld.getYear() * 10000) + (ld.getMonthValue() * 100) + ld.getDayOfMonth();
    }

    private static int time2int(java.sql.Time time) {
        final LocalTime lt = time.toLocalTime();
        return 3600 * lt.getHour() + 60 * lt.getMinute() + lt.getSecond();
    }

    private static String date2str(java.sql.Date date) {
        final LocalDate ld = date.toLocalDate();
        return String.format("%d/%02d/%02d", ld.getYear(), ld.getMonthValue(), ld.getDayOfMonth());
    }

    /**
     * All the conversion modes supported.
     */
    public enum ConvMode {
        BLOB_STREAM,
        BLOB_OBJECT,
        BINARY,
        BOOL,
        INT32,
        INT64,
        UINT32,
        UINT64,
        FLOAT,
        DOUBLE,
        DECIMAL,
        DATE,
        DATETIME,
        TIMESTAMP,
        TEXT,
        DATE_INT32,
        DATE_INT64,
        DATE_UINT32,
        DATE_UINT64,
        DATE_STR,
        TIME_INT32,
        TS_DATE,
        TS_INT64,
        TS_UINT64,
        TS_STR,
        INT2BOOL,
        STR2BOOL,
        CHARSTREAM,
    }

    /**
     * Conversion settings for the particular source column
     */
    public static final class ConvInfo {
        private final int structIndex; // 0-based position in the destination StructValue
        private final ConvMode mode; // field conversion mode
        private final String blobPath; // full BLOB table path, when mode==BLOB

        public ConvInfo(int targetIndex, ConvMode mode, String blobPath) {
            this.structIndex = targetIndex;
            this.mode = mode;
            this.blobPath = blobPath;
        }

        public ConvInfo(int targetIndex, ConvMode mode) {
            this(targetIndex, mode, null);
        }

        public boolean isBlob() {
            return blobPath != null;
        }

        public int getStructIndex() {
            return structIndex;
        }

        public ConvMode getMode() {
            return mode;
        }

        public String getBlobPath() {
            return blobPath;
        }
    }
}
