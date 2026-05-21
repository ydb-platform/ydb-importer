package tech.ydb.importer.source;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import tech.ydb.importer.TableDecision;
import tech.ydb.importer.config.TableRef;

/**
 * Resolves source side split bounds and YDB PARTITION_AT_KEYS cuts for a table.
 */
final class AutoBoundsResolver {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory
            .getLogger(AutoBoundsResolver.class);

    private static final DateTimeFormatter MINMAX_TS_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    private final AnyTableLister lister;

    AutoBoundsResolver(AnyTableLister lister) {
        this.lister = lister;
    }

    void resolve(Connection con, TableDecision td, TableMetadata tm) throws SQLException {
        TableRef ref = td.getTableRef();
        Map<String, Range> cache = new HashMap<>();
        resolveYdbPartition(con, td, tm, ref, cache);
        if (ref != null) {
            resolveSplitBounds(con, td, tm, ref, cache);
        }
    }

    private void resolveSplitBounds(Connection con, TableDecision td, TableMetadata tm,
            TableRef ref, Map<String, Range> cache) throws SQLException {
        if (!ref.hasSplit()) {
            return;
        }
        if (ref.isSplitCountAuto()) {
            Integer ydbN = ref.getYdbPartitionCount();
            if (ydbN != null && ydbN >= 2) {
                ref.setSplitCount(ydbN);
            } else {
                LOG.warn("split-count=auto for {}.{} without resolvable ydb-partition-count, single reader",
                        td.getSchema(), td.getTable());
                ref.setSplitBy(null);
                return;
            }
        }
        if (ref.getSplitFrom() != null && ref.getSplitTo() != null) {
            return;
        }
        String col = ref.getSplitBy();
        ColumnInfo ci = tm.findColumn(col);
        if (ci == null) {
            throw new RuntimeException("split-by column not found: " + col
                    + " in " + td.getSchema() + "." + td.getTable());
        }
        SplitColumnType type;
        try {
            type = RangeSplitter.detectType(ci.getSqlType(), ci.getSqlScale());
        } catch (IllegalArgumentException ex) {
            throw new RuntimeException("split-by column '" + col + "' in "
                    + td.getSchema() + "." + td.getTable()
                    + " has unsupported type for range split", ex);
        }
        Range r = cachedMinMax(con, td, col, type, cache);
        if (r == null) {
            LOG.warn("Empty source table {}.{}, split disabled",
                    td.getSchema(), td.getTable());
            ref.setSplitBy(null);
            return;
        }
        ref.setSplitFrom(r.lower);
        ref.setSplitTo(r.upper);
        LOG.info("Auto split bounds for {}.{}: {} in [{}, {}]",
                td.getSchema(), td.getTable(), col, r.lower, r.upper);
    }

    private void resolveYdbPartition(Connection con, TableDecision td, TableMetadata tm,
            TableRef ref, Map<String, Range> cache) throws SQLException {
        int requestedN = td.ydbPartitionCount();
        if (requestedN == TableRef.NONE) {
            return;
        }
        boolean isAuto = (requestedN == TableRef.AUTO);
        if (tm.getKey().isEmpty()) {
            if (isAuto) {
                LOG.info("No primary key on {}.{}, YDB partitioning skipped",
                        td.getSchema(), td.getTable());
            } else {
                LOG.warn("No primary key on {}.{}, YDB partitioning skipped",
                        td.getSchema(), td.getTable());
            }
            return;
        }
        ColumnInfo leading = tm.getKey().iterator().next();
        SplitColumnType type;
        try {
            type = RangeSplitter.detectType(leading.getSqlType(), leading.getSqlScale());
        } catch (IllegalArgumentException ex) {
            if (isAuto) {
                LOG.info("Leading PK column '{}' of {}.{} has unsupported type, YDB partitioning skipped",
                        leading.getName(), td.getSchema(), td.getTable());
            } else {
                LOG.warn("Leading PK column '{}' of {}.{} has unsupported type, YDB partitioning skipped",
                        leading.getName(), td.getSchema(), td.getTable());
            }
            return;
        }
        String from = ref == null ? null : ref.getYdbPartitionFrom();
        String to = ref == null ? null : ref.getYdbPartitionTo();
        if (isAuto) {
            if (from != null || to != null) {
                LOG.warn("ydb-partition-from/to needs an explicit count for {}.{}, YDB partitioning skipped",
                        td.getSchema(), td.getTable());
                return;
            }
            if (!tryMirrorPreSplit(con, td, tm, leading.getName(), type, true, 0)) {
                LOG.info("No usable source partitions for {}.{}, YDB partitioning skipped",
                        td.getSchema(), td.getTable());
            }
            return;
        }
        if (from != null && to != null) {
            applyEqualSplit(td, tm, leading.getName(), type, from, to, requestedN);
            return;
        }
        if (tryMirrorPreSplit(con, td, tm, leading.getName(), type, false, requestedN)) {
            return;
        }
        Range r = cachedMinMax(con, td, leading.getName(), type, cache);
        if (r == null) {
            LOG.warn("Empty source table {}.{}, YDB partitioning skipped",
                    td.getSchema(), td.getTable());
            return;
        }
        applyEqualSplit(td, tm, leading.getName(), type, r.lower, r.upper, requestedN);
    }

    private boolean tryMirrorPreSplit(Connection con, TableDecision td, TableMetadata tm,
            String leadingName, SplitColumnType type, boolean isAuto, int requestedN)
            throws SQLException {
        List<TaskInfo> partitions = lister.listPartitions(con, td, tm);
        if (partitions.size() < 2) {
            return false;
        }
        List<Range> ranges = perPartitionMinMax(con, partitions, leadingName, type);
        if (ranges == null) {
            return false;
        }
        Collections.sort(ranges, (a, b) ->
                RangeSplitter.compareBounds(a.lower, b.lower, type));
        List<String> cuts = mirrorCutsOrNull(ranges, type);
        if (cuts == null) {
            return false;
        }
        if (!isAuto && requestedN != ranges.size()) {
            LOG.warn("ydb-partition-count={} but source has {} non-overlapping partitions for {}.{}, mirror skipped",
                    requestedN, ranges.size(), td.getSchema(), td.getTable());
            return false;
        }
        applyCuts(td, tm, cuts, ranges.size(),
                "mirror on '" + leadingName + "' over " + ranges.size() + " source partitions");
        return true;
    }

    private List<Range> perPartitionMinMax(Connection con, List<TaskInfo> partitions,
            String column, SplitColumnType type) throws SQLException {
        List<Range> result = new ArrayList<>(partitions.size());
        for (TaskInfo part : partitions) {
            String baseSql = part.getQueries().get(0).getSql();
            Range r = queryMinMaxOn(con, "(" + baseSql + ") subq", column, type);
            if (r == null) {
                return null;
            }
            result.add(r);
        }
        return result;
    }

    private static List<String> mirrorCutsOrNull(List<Range> sorted, SplitColumnType type) {
        List<String> cuts = new ArrayList<>(sorted.size() - 1);
        for (int i = 1; i < sorted.size(); i++) {
            if (RangeSplitter.compareBounds(
                    sorted.get(i - 1).upper, sorted.get(i).lower, type) >= 0) {
                return null;
            }
            cuts.add(sorted.get(i).lower);
        }
        return cuts;
    }

    private void applyCuts(TableDecision td, TableMetadata tm,
            List<String> cuts, int n, String strategyDesc) {
        tm.setPartitionAtKeys(cuts);
        TableRef ref = td.getTableRef();
        if (ref != null) {
            ref.setYdbPartitionCount(n);
        }
        LOG.info("YDB partitioning for {}.{}: {}, {} initial partitions",
                td.getSchema(), td.getTable(), strategyDesc, cuts.size() + 1);
    }

    private void applyEqualSplit(TableDecision td, TableMetadata tm, String leadingName,
            SplitColumnType type, String from, String to, int n) {
        try {
            List<String> cuts = RangeSplitter.computeCuts(from, to, n, type);
            applyCuts(td, tm, cuts, n,
                    "equal-split on '" + leadingName + "' [" + from + ", " + to + "] into " + n);
        } catch (IllegalArgumentException ex) {
            LOG.warn("Cannot compute YDB partitioning cuts for {}.{}: {}",
                    td.getSchema(), td.getTable(), ex.getMessage());
        }
    }

    private Range cachedMinMax(Connection con, TableDecision td, String column,
            SplitColumnType type, Map<String, Range> cache) throws SQLException {
        Range r = cache.get(column);
        if (r == null) {
            r = queryMinMax(con, td, column, type);
            if (r != null) {
                cache.put(column, r);
            }
        }
        return r;
    }

    private Range queryMinMax(Connection con, TableDecision td, String column,
            SplitColumnType type) throws SQLException {
        String source;
        if (td.getTableRef() != null && td.getTableRef().hasQueryText()) {
            source = "(" + td.getTableRef().getQueryText() + ") subq";
        } else {
            source = lister.safeId(td.getSchema()) + "." + lister.safeId(td.getTable());
        }
        return queryMinMaxOn(con, source, column, type);
    }

    private Range queryMinMaxOn(Connection con, String sourceExpr, String column,
            SplitColumnType type) throws SQLException {
        String quotedCol = lister.safeId(column);
        String sql = "SELECT min(" + quotedCol + "), max(" + quotedCol
                + ") FROM " + sourceExpr;
        try (Statement s = con.createStatement();
                ResultSet rs = s.executeQuery(sql)) {
            if (!rs.next()) {
                return null;
            }
            Object lo = rs.getObject(1);
            Object hi = rs.getObject(2);
            if (lo == null || hi == null) {
                return null;
            }
            return new Range(formatBound(lo, type), formatBound(hi, type));
        }
    }

    private static String formatBound(Object value, SplitColumnType type) {
        switch (type) {
            case INTEGER:
                if (value instanceof Number) {
                    return Long.toString(((Number) value).longValue());
                }
                return value.toString();
            case DECIMAL:
            case DOUBLE:
                if (value instanceof BigDecimal) {
                    return ((BigDecimal) value).toPlainString();
                }
                return value.toString();
            case DATE:
                if (value instanceof java.sql.Date) {
                    return ((java.sql.Date) value).toLocalDate().toString();
                }
                if (value instanceof LocalDate) {
                    return value.toString();
                }
                return value.toString();
            case TIMESTAMP:
                if (value instanceof java.sql.Timestamp) {
                    return ((java.sql.Timestamp) value).toLocalDateTime().format(MINMAX_TS_FORMAT);
                }
                if (value instanceof LocalDateTime) {
                    return ((LocalDateTime) value).format(MINMAX_TS_FORMAT);
                }
                return value.toString();
            default:
                throw new IllegalStateException("Unsupported type: " + type);
        }
    }

    private static final class Range {
        final String lower;
        final String upper;

        Range(String lower, String upper) {
            this.lower = lower;
            this.upper = upper;
        }
    }

}
