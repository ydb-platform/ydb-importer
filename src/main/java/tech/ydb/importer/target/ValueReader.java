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
import java.util.function.Function;

import tech.ydb.table.values.DecimalType;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;
import tech.ydb.table.values.VoidValue;

/**
 * Adapter for converting values of different SQL types to YDB values.
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
    private static final ValueReader DATE32 = new DateReader(date -> PrimitiveValue.newDate32(date.toLocalDate()));
    private static final ValueReader DATE_INT32 = new DateReader(date -> PrimitiveValue.newInt32(date2int(date)));
    private static final ValueReader DATE_UINT32 = new DateReader(date -> PrimitiveValue.newUint32(date2int(date)));
    private static final ValueReader DATE_INT64 = new DateReader(date -> PrimitiveValue.newInt64(date2int(date)));
    private static final ValueReader DATE_UINT64 = new DateReader(date -> PrimitiveValue.newUint64(date2int(date)));
    private static final ValueReader DATE_STR = new DateReader(date -> PrimitiveValue.newText(date2str(date)));

    private static final ValueReader TIME_INT32 = new TimeReader(time -> PrimitiveValue.newInt32(time2int(time)));

    private static final ValueReader DATETIME = new TimestampReader(
            ts -> PrimitiveValue.newDatetime(ts.toInstant()));
    private static final ValueReader TIMESTAMP = new TimestampReader(
            ts -> PrimitiveValue.newTimestamp(ts.toInstant()));
    private static final ValueReader TS_DATE = new TimestampReader(
            ts -> PrimitiveValue.newDate(ts.toLocalDateTime().toLocalDate())
    );

    private static final ValueReader TS_DATE32 = new TimestampReader(
            ts -> PrimitiveValue.newDate32(ts.toLocalDateTime().toLocalDate())
    );
    private static final ValueReader DATETIME64 = new TimestampReader(
            ts -> PrimitiveValue.newDatetime64(ts.toInstant()));
    private static final ValueReader TIMESTAMP64 = new TimestampReader(
            ts -> PrimitiveValue.newTimestamp64(ts.toInstant()));

    private static final ValueReader TS_INT64 = new TimestampReader(ts -> PrimitiveValue.newInt64(ts.getTime()));
    private static final ValueReader TS_UINT64 = new TimestampReader(ts -> PrimitiveValue.newUint64(ts.getTime()));
    private static final ValueReader TS_STR = new TimestampReader(ts -> PrimitiveValue.newText(ts.toString()));

    private static final ValueReader UUID_BINARY = new UuidReaderBinary();
    private static final ValueReader UUID_TEXT = new UuidReaderText();

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

    public void flush() {
        // Nothing
    }

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

    private static class UuidReaderText extends ValueReader {
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
                synthKey.update(value.getBytes(StandardCharsets.UTF_8));
                synthKey.updateSeparator();
            }

            return PrimitiveValue.newUuid(value);
        }
    }

    private static class UuidReaderBinary extends ValueReader {
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

            ByteBuffer byteBuffer = ByteBuffer.wrap(value);
            long high = byteBuffer.getLong();
            long low = byteBuffer.getLong();
            return PrimitiveValue.newUuid(new UUID(high, low));
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
}
