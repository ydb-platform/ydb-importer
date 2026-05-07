package tech.ydb.importer.integration.verification;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;

/** Column types used by scenarios */
public enum LogicalType {

    INT32(java.sql.Types.INTEGER),
    INT64(java.sql.Types.BIGINT),
    DECIMAL_18_4(java.sql.Types.DECIMAL),
    STRING(java.sql.Types.VARCHAR),
    BOOL(java.sql.Types.BOOLEAN),
    DATE(java.sql.Types.DATE),
    DATETIME(java.sql.Types.TIMESTAMP);

    private final int sqlType;

    LogicalType(int sqlType) {
        this.sqlType = sqlType;
    }

    public void set(PreparedStatement ps, int idx, Object value)
            throws SQLException {
        if (value == null) {
            ps.setNull(idx, sqlType);
            return;
        }
        switch (this) {
            case INT32:
                ps.setInt(idx, (Integer) value);
                break;
            case INT64:
                ps.setLong(idx, (Long) value);
                break;
            case DECIMAL_18_4:
                ps.setBigDecimal(idx, (BigDecimal) value);
                break;
            case STRING:
                ps.setString(idx, (String) value);
                break;
            case BOOL:
                ps.setBoolean(idx, (Boolean) value);
                break;
            case DATE:
                ps.setDate(idx,
                        java.sql.Date.valueOf((LocalDate) value));
                break;
            case DATETIME:
                ps.setTimestamp(idx,
                        java.sql.Timestamp.valueOf((LocalDateTime) value));
                break;
            default:
                throw new IllegalArgumentException("Unsupported type: " + this);
        }
    }
}
