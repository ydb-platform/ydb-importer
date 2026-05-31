package tech.ydb.importer.source;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import tech.ydb.importer.TableDecision;
import tech.ydb.importer.config.TableRef;
import tech.ydb.importer.source.RangeSplitter.Range;
import tech.ydb.importer.target.YdbTypeMapper;

/**
 * Resolves source side split bounds and YDB PARTITION_AT_KEYS cuts for a table.
 */
final class AutoBoundsResolver {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory
            .getLogger(AutoBoundsResolver.class);

    private final AnyTableLister lister;

    AutoBoundsResolver(AnyTableLister lister) {
        this.lister = lister;
    }

    void resolve(Connection con, TableDecision td, TableMetadata tm) throws SQLException {
        TableRef ref = td.getTableRef();
        Map<String, Range> cache = new HashMap<>();
        if (ref != null) {
            resolveSplitByAuto(td, tm, ref);
        }
        resolveYdbPartition(con, td, tm, ref, cache);
        if (ref != null) {
            resolveSplitBounds(con, td, tm, ref, cache);
        }
    }

    /** Picks the leading key as the auto split column. */
    private void resolveSplitByAuto(TableDecision td, TableMetadata tm, TableRef ref) {
        if (!ref.isSplitByAuto()) {
            return;
        }
        if (tm.getKey().isEmpty()) {
            ref.setSplitBy(null);
            return;
        }
        ColumnInfo leading = tm.getKey().iterator().next();
        try {
            RangeSplitter.detectType(leading.getSqlType(), leading.getSqlScale());
        } catch (IllegalArgumentException ex) {
            ref.setSplitBy(null);
            return;
        }
        ref.setSplitBy(leading.getName());
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
                LOG.warn("{}.{}: split-count=auto has no ydb-partition-count to take the count from",
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
            ref.setSplitBy(null);
            return;
        }
        ref.setSplitFrom(r.lower);
        ref.setSplitTo(r.upper);
    }

    private void resolveYdbPartition(Connection con, TableDecision td, TableMetadata tm,
            TableRef ref, Map<String, Range> cache) throws SQLException {
        int requestedN = td.ydbPartitionCount();
        if (requestedN == TableRef.NONE) {
            return;
        }
        boolean isAuto = (requestedN == TableRef.AUTO);
        if (tm.getKey().isEmpty()) {
            logSkip(isAuto, td, "No key on {}.{}, YDB partitioning skipped");
            return;
        }
        ColumnInfo leading = tm.getKey().iterator().next();
        SplitColumnType type;
        try {
            type = RangeSplitter.detectType(leading.getSqlType(), leading.getSqlScale());
        } catch (IllegalArgumentException ex) {
            logSkip(isAuto, td, "Leading key column of {}.{} has unsupported type, YDB partitioning skipped");
            return;
        }
        if (type != SplitColumnType.INTEGER) {
            logSkip(isAuto, td, "Leading key of {}.{} is not an integer type, "
                    + "YDB partitioning skipped");
            return;
        }
        if (!YdbTypeMapper.partitionableInteger(leading, td.getOptions())) {
            logSkip(isAuto, td, "Leading key of {}.{} does not map to an integer YDB type, "
                    + "YDB partitioning skipped");
            return;
        }
        String from = ref == null ? null : ref.getYdbPartitionFrom();
        String to = ref == null ? null : ref.getYdbPartitionTo();

        if (!isAuto) {
            String lower = from;
            String upper = to;
            String strategy = "by ydb key range";
            if (lower == null || upper == null) {
                Range r = cachedMinMax(con, td, leading.getName(), type, cache);
                if (r == null) {
                    return;
                }
                lower = r.lower;
                upper = r.upper;
                strategy = "by source key range";
            }
            applyEqualSplit(td, tm, type, lower, upper, requestedN, strategy);
            return;
        }

        // auto
        if (from != null || to != null) {
            LOG.warn("ydb-partition-from/to needs a numeric ydb-partition-count for {}.{}, YDB partitioning skipped",
                    td.getSchema(), td.getTable());
            return;
        }
        // split-count sets the read parallelism, match the target partitions to it.
        if (ref != null && ref.hasSplit() && ref.getSplitCount() >= 2) {
            Range r = cachedMinMax(con, td, leading.getName(), type, cache);
            if (r == null) {
                return;
            }
            applyEqualSplit(td, tm, type, r.lower, r.upper, ref.getSplitCount(),
                    "by source key range");
            return;
        }
        // mirror source partitions with disjoint key ranges.
        List<TaskInfo> partitions = lister.listPartitions(con, td, tm);
        tryMirrorPreSplit(con, td, tm, partitions, leading.getName(), type);
    }

    private void logSkip(boolean isAuto, TableDecision td, String msg) {
        if (!isAuto) {
            LOG.warn(msg, td.getSchema(), td.getTable());
        }
    }

    /** Mirrors YDB boundaries from native partitions with disjoint key ranges. */
    private boolean tryMirrorPreSplit(Connection con, TableDecision td, TableMetadata tm,
            List<TaskInfo> partitions, String leadingName, SplitColumnType type)
            throws SQLException {
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
        applyCuts(td, tm, cuts, ranges.size(), "by source partitions");
        return true;
    }

    private List<Range> perPartitionMinMax(Connection con, List<TaskInfo> partitions,
            String column, SplitColumnType type) throws SQLException {
        List<Range> result = new ArrayList<>(partitions.size());
        for (TaskInfo part : partitions) {
            String baseSql = part.getQueries().get(0).getSql();
            Range r = lister.queryMinMaxOn(con, "(" + baseSql + ") subq", column, type);
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
            List<String> cuts, int n, String strategy) {
        tm.setPartitionAtKeys(cuts);
        tm.setPartitionStrategy(strategy);
        TableRef ref = td.getTableRef();
        if (ref != null) {
            ref.setYdbPartitionCount(n);
        }
    }

    private void applyEqualSplit(TableDecision td, TableMetadata tm,
            SplitColumnType type, String from, String to, int n, String strategy) {
        try {
            List<String> cuts = RangeSplitter.computeCuts(from, to, n, type);
            applyCuts(td, tm, cuts, n, strategy);
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
        return lister.queryMinMaxOn(con, source, column, type);
    }

}
