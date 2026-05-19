package tech.ydb.importer.source;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import tech.ydb.importer.TableDecision;
import tech.ydb.importer.config.TableRef;

/**
 * Builds parallel reading tasks from a range split configuration.
 */
final class RangeSplitter {

    private static final DateTimeFormatter TS_PARSE = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd")
            .optionalStart()
                .appendLiteral(' ')
                .appendPattern("HH:mm:ss")
                .optionalStart()
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                .optionalEnd()
            .optionalEnd()
            .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
            .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
            .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
            .toFormatter();

    private static final DateTimeFormatter TS_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    private RangeSplitter() {
    }

    static List<TaskInfo> generate(TableDecision td, TableMetadata tm, AnyTableLister lister) {
        TableRef ref = td.getTableRef();
        ColumnInfo col = tm.findColumn(ref.getSplitBy());
        if (col == null) {
            throw new IllegalArgumentException(
                    "split-by column '" + ref.getSplitBy()
                    + "' not found in table " + td.getSchema() + "." + td.getTable()
                    + " (if using <query>, the split-by column must be in the SELECT list)");
        }
        SplitColumnType type = detectType(col.getSqlType());
        int count = ref.getSplitCount();
        List<String> cuts = computeCuts(ref.getSplitFrom(), ref.getSplitTo(), count, type);

        String baseSql = ref.hasQueryText()
                ? "SELECT * FROM (" + ref.getQueryText() + ") subq"
                : lister.makeSelectSql(td.getSchema(), td.getTable(), tm.getColumns());
        String quotedCol = lister.safeId(ref.getSplitBy());

        List<TaskInfo> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String where = buildWhere(i, count, quotedCol, cuts, type, lister);
            String sql = baseSql + " WHERE " + where;
            String label = td.getSchema() + "." + td.getTable() + "#split" + i;
            result.add(new TaskInfo(label, sql));
        }
        return result;
    }

    private static SplitColumnType detectType(int sqlType) {
        switch (sqlType) {
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
                return SplitColumnType.INTEGER;
            case Types.DECIMAL:
            case Types.NUMERIC:
                return SplitColumnType.DECIMAL;
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
                return SplitColumnType.DOUBLE;
            case Types.DATE:
                return SplitColumnType.DATE;
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return SplitColumnType.TIMESTAMP;
            default:
                throw new IllegalArgumentException(
                        "Unsupported split-by column SQL type: " + sqlType);
        }
    }

    private static List<String> computeCuts(String lower, String upper, int count,
            SplitColumnType type) {
        switch (type) {
            case INTEGER:   return computeIntegerCuts(lower, upper, count);
            case DECIMAL:
            case DOUBLE:    return computeDecimalCuts(lower, upper, count);
            case DATE:      return computeDateCuts(lower, upper, count);
            case TIMESTAMP: return computeTimestampCuts(lower, upper, count);
            default:        throw new IllegalStateException();
        }
    }

    private static List<String> computeIntegerCuts(String lower, String upper, int count) {
        long lo = parseLong(lower, "split-from");
        long hi = parseLong(upper, "split-to");
        requireOrdered(lo < hi, lower, upper);
        long stride = (hi - lo) / count;
        requirePositiveStride(stride > 0, lower, upper, count);
        List<String> cuts = new ArrayList<>(count - 1);
        for (int i = 1; i < count; i++) {
            cuts.add(Long.toString(lo + stride * i));
        }
        return cuts;
    }

    private static List<String> computeDecimalCuts(String lower, String upper, int count) {
        BigDecimal lo = parseBigDecimal(lower, "split-from");
        BigDecimal hi = parseBigDecimal(upper, "split-to");
        requireOrdered(lo.compareTo(hi) < 0, lower, upper);
        BigDecimal stride = hi.subtract(lo)
                .divide(BigDecimal.valueOf(count), 18, RoundingMode.HALF_EVEN);
        requirePositiveStride(stride.signum() > 0, lower, upper, count);
        List<String> cuts = new ArrayList<>(count - 1);
        for (int i = 1; i < count; i++) {
            cuts.add(lo.add(stride.multiply(BigDecimal.valueOf(i))).toPlainString());
        }
        return cuts;
    }

    private static List<String> computeDateCuts(String lower, String upper, int count) {
        LocalDate lo = parseDate(lower, "split-from");
        LocalDate hi = parseDate(upper, "split-to");
        requireOrdered(lo.isBefore(hi), lower, upper);
        long stride = ChronoUnit.DAYS.between(lo, hi) / count;
        requirePositiveStride(stride > 0, lower, upper, count);
        List<String> cuts = new ArrayList<>(count - 1);
        for (int i = 1; i < count; i++) {
            cuts.add(lo.plusDays(stride * i).toString());
        }
        return cuts;
    }

    private static List<String> computeTimestampCuts(String lower, String upper, int count) {
        LocalDateTime lo = parseTimestamp(lower, "split-from");
        LocalDateTime hi = parseTimestamp(upper, "split-to");
        requireOrdered(lo.isBefore(hi), lower, upper);
        long stride = ChronoUnit.MICROS.between(lo, hi) / count;
        requirePositiveStride(stride > 0, lower, upper, count);
        List<String> cuts = new ArrayList<>(count - 1);
        for (int i = 1; i < count; i++) {
            cuts.add(lo.plus(stride * i, ChronoUnit.MICROS).format(TS_FORMAT));
        }
        return cuts;
    }

    private static void requireOrdered(boolean ordered, String lower, String upper) {
        if (!ordered) {
            throw new IllegalArgumentException(
                    "split-from must be less than split-to: '"
                    + lower + "' >= '" + upper + "'");
        }
    }

    private static void requirePositiveStride(boolean positive, String lower, String upper,
            int count) {
        if (!positive) {
            throw new IllegalArgumentException(
                    "Cannot split range '" + lower + "' to '" + upper
                    + "' into " + count + " parts");
        }
    }

    private static long parseLong(String value, String fieldName) {
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(
                    fieldName + " must be a valid integer: " + value, ex);
        }
    }

    private static BigDecimal parseBigDecimal(String value, String fieldName) {
        try {
            return new BigDecimal(value.trim());
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(
                    fieldName + " must be a valid decimal number: " + value, ex);
        }
    }

    private static LocalDate parseDate(String value, String fieldName) {
        try {
            return LocalDate.parse(value.trim());
        } catch (DateTimeParseException ex) {
            throw new IllegalArgumentException(
                    fieldName + " must be in format yyyy-MM-dd: " + value, ex);
        }
    }

    private static LocalDateTime parseTimestamp(String value, String fieldName) {
        try {
            return LocalDateTime.parse(value.trim(), TS_PARSE);
        } catch (DateTimeParseException ex) {
            throw new IllegalArgumentException(
                    fieldName + " must be in format yyyy-MM-dd [HH:mm:ss[.fraction]]: "
                    + value, ex);
        }
    }

    private static String buildWhere(int i, int count, String quotedCol, List<String> cuts,
            SplitColumnType type, AnyTableLister lister) {
        if (i == 0) {
            return quotedCol + " < " + lister.formatLiteral(type, cuts.get(0))
                    + " OR " + quotedCol + " IS NULL";
        }
        if (i == count - 1) {
            return quotedCol + " >= " + lister.formatLiteral(type, cuts.get(count - 2));
        }
        return quotedCol + " >= " + lister.formatLiteral(type, cuts.get(i - 1))
                + " AND " + quotedCol + " < " + lister.formatLiteral(type, cuts.get(i));
    }
}
