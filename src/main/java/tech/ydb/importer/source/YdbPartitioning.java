package tech.ydb.importer.source;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Target partitioning of a YDB table.
 */
public final class YdbPartitioning {

    public enum Mode { DEFAULT, KEY_RANGE, HASH }

    private static final YdbPartitioning DEFAULT =
            new YdbPartitioning(Mode.DEFAULT, Collections.emptyList(), 0, null, null, false);

    private final Mode mode;
    private final List<String> cuts;
    private final int hashPartitions;
    private final String hashColumn;
    private final String strategy;
    private final boolean onePartitionPerTask;

    private YdbPartitioning(Mode mode, List<String> cuts, int hashPartitions,
            String hashColumn, String strategy, boolean onePartitionPerTask) {
        this.mode = mode;
        this.cuts = cuts;
        this.hashPartitions = hashPartitions;
        this.hashColumn = hashColumn;
        this.strategy = strategy;
        this.onePartitionPerTask = onePartitionPerTask;
    }

    /** Automatic YDB partitioning. */
    public static YdbPartitioning ydbDefault() {
        return DEFAULT;
    }

    /** Row table split by key boundaries. */
    public static YdbPartitioning keyRange(List<String> cuts, String strategy,
            boolean onePartitionPerTask) {
        return new YdbPartitioning(Mode.KEY_RANGE,
                Collections.unmodifiableList(new ArrayList<>(cuts)), 0, null, strategy,
                onePartitionPerTask);
    }

    /** Column table split by key hash. */
    public static YdbPartitioning hash(int partitions, String hashColumn) {
        return new YdbPartitioning(Mode.HASH, Collections.emptyList(),
                partitions, hashColumn, "by key hash", false);
    }

    public boolean isKeyRange() {
        return mode == Mode.KEY_RANGE;
    }

    public boolean isHash() {
        return mode == Mode.HASH;
    }

    /** Key boundary values. */
    public List<String> getCuts() {
        return cuts;
    }

    /** Number of HASH partitions. */
    public int getHashPartitions() {
        return hashPartitions;
    }

    /** Column used as the HASH partitioning key. */
    public String getHashColumn() {
        return hashColumn;
    }

    public String getStrategy() {
        return strategy;
    }

    /** Read slices and YDB partitions have the same key boundaries. */
    public boolean isOnePartitionPerTask() {
        return onePartitionPerTask;
    }
}
