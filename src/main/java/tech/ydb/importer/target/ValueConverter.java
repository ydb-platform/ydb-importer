package tech.ydb.importer.target;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.time.LocalDate;
import java.time.LocalTime;

import tech.ydb.table.values.DecimalType;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;
import tech.ydb.table.values.VoidValue;

/**
 *
 * @author zinal
 */
public abstract class ValueConverter {

    public static final DecimalType DECIMAL_TYPE = DecimalType.of(22, 9);
    public static final boolean DECIMAL_ISSUE1
            = (DECIMAL_TYPE.newValue(BigDecimal.ONE).toBigDecimal()
                    .compareTo(BigDecimal.ONE) != 0);

    public static ConvMode chooseMode(int ixSource, int ixTarget,
            StructType paramListType, ResultSetMetaData rsmd) throws Exception {
        Type paramType = paramListType.getMemberType(ixTarget);
        while (Type.Kind.OPTIONAL.equals(paramType.getKind())) {
            paramType = paramType.unwrapOptional();
        }

        final int sourceType = rsmd.getColumnType(ixSource);

        switch (paramType.getKind()) {
            case DECIMAL:
                return ConvMode.DECIMAL;
            case PRIMITIVE:
                try {
                    switch ((PrimitiveType) paramType) {
                        case Bool:
                            switch (sourceType) {
                                case java.sql.Types.SMALLINT:
                                case java.sql.Types.INTEGER:
                                case java.sql.Types.BIGINT:
                                case java.sql.Types.DECIMAL:
                                case java.sql.Types.NUMERIC:
                                case java.sql.Types.FLOAT:
                                case java.sql.Types.DOUBLE:
                                    return ConvMode.INT2BOOL;
                                case java.sql.Types.CHAR:
                                case java.sql.Types.NCHAR:
                                case java.sql.Types.VARCHAR:
                                case java.sql.Types.NVARCHAR:
                                    return ConvMode.STR2BOOL;
                                default:
                                    return ConvMode.BOOL;
                            }
                        case Date:
                            switch (sourceType) {
                                case java.sql.Types.TIMESTAMP:
                                    return ConvMode.TS_DATE;
                                default:
                                    return ConvMode.DATE;
                            }
                        case Datetime:
                            return ConvMode.DATETIME;
                        case Timestamp:
                            return ConvMode.TIMESTAMP;
                        case Float:
                            return ConvMode.FLOAT;
                        case Double:
                            return ConvMode.DOUBLE;
                        case Int32:
                            switch (sourceType) {
                                case java.sql.Types.TIME:
                                    return ConvMode.TIME_INT32;
                                case java.sql.Types.DATE:
                                    return ConvMode.DATE_INT32;
                                default:
                                    return ConvMode.INT32;
                            }
                        case Int64:
                            switch (sourceType) {
                                case java.sql.Types.DATE:
                                    return ConvMode.DATE_INT64;
                                case java.sql.Types.TIMESTAMP:
                                    return ConvMode.TS_INT64;
                                default:
                                    return ConvMode.INT64;
                            }
                        case Uint32:
                            switch (sourceType) {
                                case java.sql.Types.DATE:
                                    return ConvMode.DATE_UINT32;
                                default:
                                    return ConvMode.UINT32;
                            }
                        case Uint64:
                            switch (sourceType) {
                                case java.sql.Types.DATE:
                                    return ConvMode.DATE_UINT64;
                                case java.sql.Types.TIMESTAMP:
                                    return ConvMode.TS_UINT64;
                                default:
                                    return ConvMode.UINT64;
                            }
                        case Text:
                            switch (sourceType) {
                                case java.sql.Types.DATE:
                                    return ConvMode.DATE_STR;
                                case java.sql.Types.TIMESTAMP:
                                    return ConvMode.TS_STR;
                                default:
                                    return ConvMode.TEXT;
                            }
                        case Bytes: // SQL BINARY, VARBINARY
                            return ConvMode.BINARY;
                        default:
                            throw new IllegalArgumentException("unsupported type: " + paramType);
                    }
                } catch (Exception ex) {
                    throw new IllegalArgumentException("chooseMode() failed", ex);
                }
            default:
                throw new IllegalArgumentException("chooseMode(): " + paramType + " - unsupported kind");
        }
    }

    public abstract PrimitiveValue convertBlob(ResultSet rs, ConvInfo ci) throws Exception;

    /**
     * Converts a single source ResultSet column value to YDB format.
     *
     * @param rs Input source result set
     * @param ci Column positions and conversion mode
     * @return YDB-formatted value
     * @throws Exception
     */
    public Value<?> convertValue(ResultSet rs, ConvInfo ci) throws Exception {
        final int srcpos = ci.sourceIndex;
        Object value = rs.getObject(srcpos);
        if (value == null) {
            return VoidValue.of();
        }
        switch (ci.mode) {
            case BLOB_STREAM:
            case BLOB_OBJECT:
                // Generate id + subrecords
                return convertBlob(rs, ci);
            case BINARY:
                return PrimitiveValue.newBytes(rs.getBytes(srcpos));
            case BOOL:
                return PrimitiveValue.newBool(rs.getBoolean(srcpos));
            case INT2BOOL:
                return PrimitiveValue.newBool((rs.getInt(srcpos) != 0));
            case STR2BOOL:
                return PrimitiveValue.newBool(str2bool(rs.getString(srcpos)));
            case DATE:
                return PrimitiveValue.newDate(rs.getDate(srcpos).toLocalDate());
            case DATE_INT32:
                return PrimitiveValue.newInt32(date2int(rs.getDate(srcpos)));
            case DATE_UINT32:
                return PrimitiveValue.newUint32(date2int(rs.getDate(srcpos)));
            case DATE_INT64:
                return PrimitiveValue.newInt64(date2int(rs.getDate(srcpos)));
            case DATE_UINT64:
                return PrimitiveValue.newUint64(date2int(rs.getDate(srcpos)));
            case DATE_STR:
                return PrimitiveValue.newText(date2str(rs.getDate(srcpos)));
            case TIME_INT32: {
                LocalTime ltv = rs.getTime(srcpos).toLocalTime();
                return PrimitiveValue.newInt32(ltv.getSecond()
                        + 60 * (ltv.getMinute() + 60 * ltv.getHour()));
            }
            case DATETIME:
                return PrimitiveValue.newDatetime(rs.getTimestamp(srcpos).toInstant());
            case TIMESTAMP:
                return PrimitiveValue.newTimestamp(rs.getTimestamp(srcpos).toInstant());
            case TS_DATE:
                return PrimitiveValue.newDate(rs.getTimestamp(srcpos).toLocalDateTime().toLocalDate());
            case TS_INT64:
                return PrimitiveValue.newInt64(rs.getTimestamp(srcpos).getTime());
            case TS_UINT64:
                return PrimitiveValue.newUint64(rs.getTimestamp(srcpos).getTime());
            case TS_STR:
                return PrimitiveValue.newText(rs.getTimestamp(srcpos).toString());
            case FLOAT:
                return PrimitiveValue.newFloat(rs.getFloat(srcpos));
            case DOUBLE:
                return PrimitiveValue.newDouble(rs.getDouble(srcpos));
            case INT32:
                return PrimitiveValue.newInt32(rs.getInt(srcpos));
            case UINT32:
                return PrimitiveValue.newUint32(rs.getInt(srcpos));
            case INT64:
                return PrimitiveValue.newInt64(rs.getLong(srcpos));
            case UINT64:
                return PrimitiveValue.newUint64(rs.getLong(srcpos));
            case TEXT:
                return PrimitiveValue.newText(rs.getString(srcpos));
            case DECIMAL:
                if (DECIMAL_ISSUE1) {
                    // FIX: conversion does not work properly, defect in Java YDB SDK
                    return DECIMAL_TYPE.newValue(rs.getString(srcpos));
                } else {
                    return DECIMAL_TYPE.newValue(rs.getBigDecimal(srcpos));
                }
            default:
                throw new IllegalArgumentException("Unsupported conversion: " + ci.mode);
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
        if ("N".equalsIgnoreCase(value)
                || "0".equalsIgnoreCase(value)
                || "F".equalsIgnoreCase(value)
                || "Н".equalsIgnoreCase(value)
                || "Л".equalsIgnoreCase(value)) {
            return false;
        }
        return true;
    }

    private static int date2int(java.sql.Date date) {
        final LocalDate ld = date.toLocalDate();
        return (ld.getYear() * 10000) + (ld.getMonthValue() * 100) + ld.getDayOfMonth();
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
        STR2BOOL

    }

    /**
     * Conversion settings for the particular source column
     */
    public static final class ConvInfo {

        private final int sourceIndex; // 1-based position in the input ResultSet
        private final int targetIndex; // 0-based position in the destination StructValue
        private final ConvMode mode; // field conversion mode
        private final BlobSaver blobSaver; // blob saver, when mode==BLOB

        public ConvInfo(int sourceIndex, int targetIndex, ConvMode mode, BlobSaver blobSaver) {
            this.sourceIndex = sourceIndex;
            this.targetIndex = targetIndex;
            this.mode = mode;
            this.blobSaver = blobSaver;
        }

        public ConvInfo(int sourceIndex, int targetIndex, ConvMode mode) {
            this(sourceIndex, targetIndex, mode, null);
        }

        public int getSourceIndex() {
            return sourceIndex;
        }

        public int getTargetIndex() {
            return targetIndex;
        }

        public ConvMode getMode() {
            return mode;
        }

        public BlobSaver getBlobSaver() {
            return blobSaver;
        }
    }

}
