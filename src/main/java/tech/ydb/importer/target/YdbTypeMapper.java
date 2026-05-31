package tech.ydb.importer.target;

import tech.ydb.importer.config.TableOptions;
import tech.ydb.importer.source.ColumnInfo;
import tech.ydb.table.values.DecimalType;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.Type;

/**
 * Maps a source JDBC column to its YDB column type.
 */
public final class YdbTypeMapper {

    private YdbTypeMapper() {
    }

    /**
     * Resolves a source column to its YDB type from its JDBC type, scale and the
     * table options.
     */
    public static Type convertType(ColumnInfo ci, TableOptions options) {
        switch (ci.getSqlType()) {
            case java.sql.Types.BOOLEAN:
            case java.sql.Types.BIT:
                return PrimitiveType.Bool;
            case java.sql.Types.TINYINT:
                return PrimitiveType.Int32;
            case java.sql.Types.SMALLINT:
                return PrimitiveType.Int32;
            case java.sql.Types.INTEGER:
                return PrimitiveType.Int32;
            case java.sql.Types.BIGINT:
                return PrimitiveType.Int64;
            case java.sql.Types.DECIMAL:
            case java.sql.Types.NUMERIC:
                if (ci.getSqlScale() < 0) {
                    return PrimitiveType.Double;
                }
                if (ci.getSqlScale() == 0) {
                    if (ci.getSqlPrecision() == 0) {
                        return PrimitiveType.Double;
                    }
                    if (ci.getSqlPrecision() < 10) {
                        return PrimitiveType.Int32;
                    }
                    if (ci.getSqlPrecision() < 20) {
                        return PrimitiveType.Int64;
                    }
                    if (options.isAllowCustomDecimal()) {
                        return DecimalType.of(35, 0);
                    }
                    return PrimitiveType.Int64;
                } else {
                    if (options.isAllowCustomDecimal()) {
                        int prec = ci.getSqlPrecision();
                        if (prec > 35) {
                            prec = 35;
                        }
                        int scale = ci.getSqlScale();
                        if (scale > prec) {
                            scale = prec;
                        }
                        return DecimalType.of(prec, scale);
                    }
                    return DecimalType.getDefault();
                }
            case java.sql.Types.DOUBLE:
                return PrimitiveType.Double;
            case java.sql.Types.FLOAT:
            case java.sql.Types.REAL:
                return PrimitiveType.Float;
            case java.sql.Types.VARCHAR:
            case java.sql.Types.CHAR:
            case java.sql.Types.NVARCHAR:
            case java.sql.Types.NCHAR:
            case java.sql.Types.LONGNVARCHAR:
            case java.sql.Types.LONGVARCHAR:
            case java.sql.Types.CLOB: // MAYBE: store in a separate table, like BLOBS
                return PrimitiveType.Text;
            case java.sql.Types.BINARY:
            case java.sql.Types.VARBINARY:
                return PrimitiveType.Bytes;
            case java.sql.Types.BLOB:
            case java.sql.Types.LONGVARBINARY:
            case java.sql.Types.SQLXML:
                return PrimitiveType.Int64; // Id of record sequence in the separate table.
            case java.sql.Types.DATE:
                switch (options.getDateConv()) {
                    case DATE_NEW:
                        return PrimitiveType.Date32;
                    case DATE:
                        return PrimitiveType.Date;
                    case INT:
                        return PrimitiveType.Int32;
                    case STR:
                        return PrimitiveType.Text;
                    default: {
                        /* noop */
                    }
                }
                return PrimitiveType.Date32;
            case java.sql.Types.TIME:
                return PrimitiveType.Int32;
            case java.sql.Types.TIMESTAMP:
                switch (options.getTimestampConv()) {
                    case DATE_NEW:
                        if (ci.getSqlScale() == 0) {
                            return PrimitiveType.Datetime64;
                        }
                        return PrimitiveType.Timestamp64;
                    case DATE:
                        if (ci.getSqlScale() == 0) {
                            return PrimitiveType.Datetime;
                        }
                        return PrimitiveType.Timestamp;
                    case INT:
                        return PrimitiveType.Uint64;
                    case STR:
                        return PrimitiveType.Text;
                    default: {
                        /* noop */
                    }
                }
                if (ci.getSqlScale() == 0) {
                    return PrimitiveType.Datetime64;
                }
                return PrimitiveType.Timestamp64;
            default: {
                /* noop */
            }
        }
        if (options.isSkipUnknownTypes()) {
            return null;
        }
        throw new IllegalArgumentException("Unsupported type code: " + ci.getSqlType());
    }

    /**
     * Checks that the column maps to a YDB Int or Uint type.
     */
    public static boolean partitionableInteger(ColumnInfo ci, TableOptions options) {
        Type type = convertType(ci, options);
        if (type == null || type.getKind() != Type.Kind.PRIMITIVE) {
            return false;
        }
        switch ((PrimitiveType) type) {
            case Int8:
            case Int16:
            case Int32:
            case Int64:
            case Uint8:
            case Uint16:
            case Uint32:
            case Uint64:
                return true;
            default:
                return false;
        }
    }
}
