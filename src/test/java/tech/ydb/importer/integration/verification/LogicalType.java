package tech.ydb.importer.integration.verification;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;

public enum LogicalType {

    INT32,
    INT64,
    DECIMAL_18_4,
    STRING,
    BOOL,
    DATE,
    DATETIME,
    NULLABLE_STRING;

    public void bind(PreparedStatement ps, int idx, Object value)
            throws SQLException {
        if (value == null) {
            ps.setNull(idx, java.sql.Types.VARCHAR);
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
            case NULLABLE_STRING:
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
