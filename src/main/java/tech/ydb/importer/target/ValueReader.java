package tech.ydb.importer.target;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;

import tech.ydb.table.values.DecimalType;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.Type;

/**
 * Adapter for converting values of different SQL types to YDB values.
 * Each subclass reads from a JDBC ResultSet and writes the converted value
 * via a {@link ValueWriter}, ensuring identical semantics for both
 * row-based and Arrow-based import paths.
 *
 * @author zinal
 */
public abstract class ValueReader {

    private static final ValueReader BOOL = new BoolReader();
    private static final ValueReader INT_BOOL = new IntReader(
            (w, i, v) -> w.writeBool(i, v != 0),
            SynthKey::hashInt
    );
    private static final ValueReader STR_BOOL = new StringReader(
            (w, i, v) -> w.writeBool(i, str2bool(v))
    );

    private static final ValueReader TEXT = new StringReader(ValueWriter::writeText);
    private static final ValueReader BYTES = new BytesReader();

    private static final ValueReader FLOAT = new FloatReader();
    private static final ValueReader DOUBLE = new DoubleReader();

    private static final ValueReader INT32 = new IntReader(
            ValueWriter::writeInt32, SynthKey::hashInt);
    private static final ValueReader UINT32 = new LongReader(ValueWriter::writeUint32);
    private static final ValueReader INT64 = new LongReader(ValueWriter::writeInt64);
    private static final ValueReader UINT64 = new LongReader(ValueWriter::writeUint64);

    private static final ValueReader DATE = new DateReader(
            (w, i, v) -> w.writeDate(i, v.toLocalDate()));
    private static final ValueReader DATE32 = new DateReader(
            (w, i, v) -> w.writeDate32(i, v.toLocalDate()));
    private static final ValueReader DATE_INT32 = new DateReader(
            (w, i, v) -> w.writeInt32(i, date2int(v)));
    private static final ValueReader DATE_UINT32 = new DateReader(
            (w, i, v) -> w.writeUint32(i, date2int(v)));
    private static final ValueReader DATE_INT64 = new DateReader(
            (w, i, v) -> w.writeInt64(i, date2int(v)));
    private static final ValueReader DATE_UINT64 = new DateReader(
            (w, i, v) -> w.writeUint64(i, date2int(v)));
    private static final ValueReader DATE_STR = new DateReader(
            (w, i, v) -> w.writeText(i, date2str(v)));

    private static final ValueReader TIME_INT32 = new TimeReader();

    private static final ValueReader DATETIME = new TimestampReader(
            (w, i, v) -> w.writeDatetime(i, v.toInstant()));
    private static final ValueReader TIMESTAMP = new TimestampReader(
            (w, i, v) -> w.writeTimestamp(i, v.toInstant()));
    private static final ValueReader DATETIME64 = new TimestampReader(
            (w, i, v) -> w.writeDatetime64(i, v.toInstant()));
    private static final ValueReader TIMESTAMP64 = new TimestampReader(
            (w, i, v) -> w.writeTimestamp64(i, v.toInstant()));

    private static final ValueReader TS_DATE = new TimestampReader(
            (w, i, v) -> w.writeDate(i, v.toLocalDateTime().toLocalDate()));
    private static final ValueReader TS_DATE32 = new TimestampReader(
            (w, i, v) -> w.writeDate32(i, v.toLocalDateTime().toLocalDate()));

    private static final ValueReader TS_INT64 = new TimestampReader(
            (w, i, v) -> w.writeInt64(i, v.getTime()));
    private static final ValueReader TS_UINT64 = new TimestampReader(
            (w, i, v) -> w.writeUint64(i, v.getTime()));
    private static final ValueReader TS_STR = new TimestampReader(
            (w, i, v) -> w.writeText(i, v.toString()));

    private static final ValueReader UUID_BINARY = new UuidBinaryReader();
    private static final ValueReader UUID_TEXT = new UuidTextReader();

    /**
     * Reads a single column from the JDBC ResultSet, converts and writes it
     * to the target via the supplied {@link ValueWriter}, and updates the
     * synthetic key hash when present.
     */
    public abstract void read(ResultSet rs, int rsIndex, int targetIndex,
                              ValueWriter writer, SynthKey synthKey) throws Exception;

    public void flush() {
    }

    public static ValueReader getReader(Type ydbType, int sqlType) throws Exception {
        Type paramType = ydbType;
        while (Type.Kind.OPTIONAL.equals(paramType.getKind())) {
            paramType = paramType.unwrapOptional();
        }

        if (paramType.getKind() == Type.Kind.DECIMAL) {
            final DecimalType theType = (DecimalType) paramType;
            return new BigDecimalReader(theType);
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
                case Bytes:
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

    private static class BoolReader extends ValueReader {
        @Override
        public void read(ResultSet rs, int rsIdx, int targetIdx,
                         ValueWriter w, SynthKey sk) throws Exception {
            boolean val = rs.getBoolean(rsIdx);
            if (rs.wasNull()) {
                w.writeNull(targetIdx);
                if (sk != null) {
                    sk.hashNull();
                }
                return;
            }
            w.writeBool(targetIdx, val);
            if (sk != null) {
                sk.hashBool(val);
            }
        }
    }

    private static class IntReader extends ValueReader {
        @FunctionalInterface
        interface IntWrite {
            void write(ValueWriter w, int index, int val);
        }

        @FunctionalInterface
        interface IntHash {
            void hash(SynthKey sk, int val);
        }

        private final IntWrite writeAction;
        private final IntHash hashAction;

        IntReader(IntWrite writeAction, IntHash hashAction) {
            this.writeAction = writeAction;
            this.hashAction = hashAction;
        }

        @Override
        public void read(ResultSet rs, int rsIdx, int targetIdx,
                         ValueWriter w, SynthKey sk) throws Exception {
            int val = rs.getInt(rsIdx);
            if (rs.wasNull()) {
                w.writeNull(targetIdx);
                if (sk != null) {
                    sk.hashNull();
                }
                return;
            }
            writeAction.write(w, targetIdx, val);
            if (sk != null) {
                hashAction.hash(sk, val);
            }
        }
    }

    private static class LongReader extends ValueReader {
        @FunctionalInterface
        interface LongWrite {
            void write(ValueWriter w, int index, long val);
        }

        private final LongWrite writeAction;

        LongReader(LongWrite writeAction) {
            this.writeAction = writeAction;
        }

        @Override
        public void read(ResultSet rs, int rsIdx, int targetIdx,
                         ValueWriter w, SynthKey sk) throws Exception {
            long val = rs.getLong(rsIdx);
            if (rs.wasNull()) {
                w.writeNull(targetIdx);
                if (sk != null) {
                    sk.hashNull();
                }
                return;
            }
            writeAction.write(w, targetIdx, val);
            if (sk != null) {
                sk.hashLong(val);
            }
        }
    }

    private static class FloatReader extends ValueReader {
        @Override
        public void read(ResultSet rs, int rsIdx, int targetIdx,
                         ValueWriter w, SynthKey sk) throws Exception {
            float val = rs.getFloat(rsIdx);
            if (rs.wasNull()) {
                w.writeNull(targetIdx);
                if (sk != null) {
                    sk.hashNull();
                }
                return;
            }
            w.writeFloat(targetIdx, val);
            if (sk != null) {
                sk.hashFloat(val);
            }
        }
    }

    private static class DoubleReader extends ValueReader {
        @Override
        public void read(ResultSet rs, int rsIdx, int targetIdx,
                         ValueWriter w, SynthKey sk) throws Exception {
            double val = rs.getDouble(rsIdx);
            if (rs.wasNull()) {
                w.writeNull(targetIdx);
                if (sk != null) {
                    sk.hashNull();
                }
                return;
            }
            w.writeDouble(targetIdx, val);
            if (sk != null) {
                sk.hashDouble(val);
            }
        }
    }

    private static class StringReader extends ValueReader {
        @FunctionalInterface
        interface StrWrite {
            void write(ValueWriter w, int index, String val);
        }

        private final StrWrite writeAction;

        StringReader(StrWrite writeAction) {
            this.writeAction = writeAction;
        }

        @Override
        public void read(ResultSet rs, int rsIdx, int targetIdx,
                         ValueWriter w, SynthKey sk) throws Exception {
            String val = rs.getString(rsIdx);
            if (rs.wasNull()) {
                w.writeNull(targetIdx);
                if (sk != null) {
                    sk.hashNull();
                }
                return;
            }
            writeAction.write(w, targetIdx, val);
            if (sk != null) {
                sk.hashString(val);
            }
        }
    }

    private static class BytesReader extends ValueReader {
        @Override
        public void read(ResultSet rs, int rsIdx, int targetIdx,
                         ValueWriter w, SynthKey sk) throws Exception {
            byte[] val = rs.getBytes(rsIdx);
            if (rs.wasNull()) {
                w.writeNull(targetIdx);
                if (sk != null) {
                    sk.hashNull();
                }
                return;
            }
            w.writeBytes(targetIdx, val);
            if (sk != null) {
                sk.hashBytes(val);
            }
        }
    }

    private static class DateReader extends ValueReader {
        @FunctionalInterface
        interface DateWrite {
            void write(ValueWriter w, int index, java.sql.Date val);
        }

        private final DateWrite writeAction;

        DateReader(DateWrite writeAction) {
            this.writeAction = writeAction;
        }

        @Override
        public void read(ResultSet rs, int rsIdx, int targetIdx,
                         ValueWriter w, SynthKey sk) throws Exception {
            java.sql.Date val = rs.getDate(rsIdx);
            if (rs.wasNull()) {
                w.writeNull(targetIdx);
                if (sk != null) {
                    sk.hashNull();
                }
                return;
            }
            writeAction.write(w, targetIdx, val);
            if (sk != null) {
                sk.hashDate(val);
            }
        }
    }

    private static class TimeReader extends ValueReader {
        @Override
        public void read(ResultSet rs, int rsIdx, int targetIdx,
                         ValueWriter w, SynthKey sk) throws Exception {
            java.sql.Time val = rs.getTime(rsIdx);
            if (rs.wasNull()) {
                w.writeNull(targetIdx);
                if (sk != null) {
                    sk.hashNull();
                }
                return;
            }
            w.writeInt32(targetIdx, time2int(val));
            if (sk != null) {
                sk.hashTime(val);
            }
        }
    }

    private static class TimestampReader extends ValueReader {
        @FunctionalInterface
        interface TsWrite {
            void write(ValueWriter w, int index, Timestamp val);
        }

        private final TsWrite writeAction;

        TimestampReader(TsWrite writeAction) {
            this.writeAction = writeAction;
        }

        @Override
        public void read(ResultSet rs, int rsIdx, int targetIdx,
                         ValueWriter w, SynthKey sk) throws Exception {
            Timestamp val = rs.getTimestamp(rsIdx);
            if (rs.wasNull()) {
                w.writeNull(targetIdx);
                if (sk != null) {
                    sk.hashNull();
                }
                return;
            }
            writeAction.write(w, targetIdx, val);
            if (sk != null) {
                sk.hashTimestamp(val);
            }
        }
    }

    private static class BigDecimalReader extends ValueReader {
        private final DecimalType decimalType;

        BigDecimalReader(DecimalType decimalType) {
            this.decimalType = decimalType;
        }

        @Override
        public void read(ResultSet rs, int rsIdx, int targetIdx,
                         ValueWriter w, SynthKey sk) throws Exception {
            BigDecimal val = rs.getBigDecimal(rsIdx);
            if (rs.wasNull()) {
                w.writeNull(targetIdx);
                if (sk != null) {
                    sk.hashNull();
                }
                return;
            }
            w.writeDecimal(targetIdx, decimalType, val);
            if (sk != null) {
                sk.hashBigDecimal(val);
            }
        }
    }

    private static class UuidTextReader extends ValueReader {
        @Override
        public void read(ResultSet rs, int rsIdx, int targetIdx,
                         ValueWriter w, SynthKey sk) throws Exception {
            String val = rs.getString(rsIdx);
            if (rs.wasNull()) {
                w.writeNull(targetIdx);
                if (sk != null) {
                    sk.hashNull();
                }
                return;
            }
            w.writeUuidFromString(targetIdx, val);
            if (sk != null) {
                sk.hashUuidText(val);
            }
        }
    }

    private static class UuidBinaryReader extends ValueReader {
        @Override
        public void read(ResultSet rs, int rsIdx, int targetIdx,
                         ValueWriter w, SynthKey sk) throws Exception {
            byte[] val = rs.getBytes(rsIdx);
            if (rs.wasNull()) {
                w.writeNull(targetIdx);
                if (sk != null) {
                    sk.hashNull();
                }
                return;
            }
            ByteBuffer bb = ByteBuffer.wrap(val);
            w.writeUuid(targetIdx, new UUID(bb.getLong(), bb.getLong()));
            if (sk != null) {
                sk.hashBytes(val);
            }
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
                || "\u041D".equalsIgnoreCase(value)
                || "\u041B".equalsIgnoreCase(value));
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
