package tech.ydb.importer.integration.common;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Assertion utilities for comparing YDB row data in tests.
 */
public final class YdbRowMatcher {

    private YdbRowMatcher() {
    }

    /**
     * Asserts that at least one row matches ALL given column-value pairs.
     *
     * @param columnValuePairs alternating column name and expected value
     */
    public static void assertRowExists(List<Map<String, Object>> rows,
                                       Object... columnValuePairs) {
        if (columnValuePairs.length % 2 != 0) {
            throw new IllegalArgumentException(
                    "columnValuePairs must have even length");
        }

        for (Map<String, Object> row : rows) {
            boolean allMatch = true;
            for (int i = 0; i < columnValuePairs.length; i += 2) {
                String col = (String) columnValuePairs[i];
                Object expected = columnValuePairs[i + 1];
                Object actual = row.get(col);
                if (!valuesEqual(expected, actual)) {
                    allMatch = false;
                    break;
                }
            }
            if (allMatch) {
                return;
            }
        }

        StringBuilder sb = new StringBuilder("No row matches {");
        for (int i = 0; i < columnValuePairs.length; i += 2) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(columnValuePairs[i]).append("=")
                    .append(describe(columnValuePairs[i + 1]));
        }
        sb.append("} among ").append(rows.size()).append(" rows");
        throw new AssertionError(sb.toString());
    }

    public static boolean valuesEqual(Object expected, Object actual) {
        if (expected == null || actual == null) {
            return expected == null && actual == null;
        }
        if (expected instanceof byte[] && actual instanceof byte[]) {
            return Arrays.equals((byte[]) expected, (byte[]) actual);
        }
        if (expected instanceof BigDecimal && actual instanceof BigDecimal) {
            return ((BigDecimal) expected).compareTo(
                    (BigDecimal) actual) == 0;
        }
        if (isIntegral(expected) && isIntegral(actual)) {
            return ((Number) expected).longValue()
                    == ((Number) actual).longValue();
        }
        Instant e = toInstant(expected);
        Instant a = toInstant(actual);
        if (e != null && a != null) {
            return e.equals(a);
        }
        return expected.equals(actual);
    }

    private static boolean isIntegral(Object v) {
        return v instanceof Byte || v instanceof Short
                || v instanceof Integer || v instanceof Long;
    }

    private static Instant toInstant(Object v) {
        if (v instanceof Instant) {
            return (Instant) v;
        }
        if (v instanceof LocalDateTime) {
            return ((LocalDateTime) v).toInstant(ZoneOffset.UTC);
        }
        if (v instanceof LocalDate) {
            return ((LocalDate) v).atStartOfDay(ZoneOffset.UTC).toInstant();
        }
        return null;
    }

    public static String describe(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof byte[]) {
            return "byte[" + ((byte[]) value).length + "]";
        }
        return value.getClass().getSimpleName() + "(" + value + ")";
    }
}
