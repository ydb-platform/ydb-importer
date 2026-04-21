package tech.ydb.importer.integration.verification;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/** Test table spec with columns, oracle, optional BLOB and partitioning */
public final class TableScenario {

    private static final String DEFAULT_KEY_COLUMN = "id";

    private final String tableName;
    private final String keyColumn;
    private final List<ColumnSpec> columns;
    private final RowOracle oracle;
    private final Set<Feature> features;
    private final String blobColumn;
    private final PartitionStyle partitionStyle;
    private final String partitionColumn;

    public TableScenario(String tableName, List<ColumnSpec> columns,
                         RowOracle oracle) {
        this(tableName, DEFAULT_KEY_COLUMN, columns, oracle, null, null, null);
    }

    public TableScenario(String tableName, List<ColumnSpec> columns,
                         RowOracle oracle, String blobColumn,
                         PartitionStyle partitionStyle,
                         String partitionColumn) {
        this(tableName, DEFAULT_KEY_COLUMN, columns, oracle,
                blobColumn, partitionStyle, partitionColumn);
    }

    public TableScenario(String tableName, String keyColumn,
                         List<ColumnSpec> columns, RowOracle oracle,
                         String blobColumn, PartitionStyle partitionStyle,
                         String partitionColumn) {
        this.tableName = tableName;
        this.keyColumn = keyColumn;
        this.columns = Collections.unmodifiableList(columns);
        this.oracle = oracle;
        this.blobColumn = blobColumn;
        this.partitionStyle = partitionStyle;
        this.partitionColumn = partitionColumn;

        EnumSet<Feature> f = EnumSet.noneOf(Feature.class);
        if (blobColumn != null) {
            f.add(Feature.BLOB);
        }
        if (partitionStyle != null) {
            f.add(Feature.PARTITIONED);
        }
        this.features = Collections.unmodifiableSet(f);
    }

    public String tableName() {
        return tableName;
    }

    public String keyColumn() {
        return keyColumn;
    }

    public List<ColumnSpec> columns() {
        return columns;
    }

    public RowOracle oracle() {
        return oracle;
    }

    public Set<Feature> features() {
        return features;
    }

    public boolean requires(Feature feature) {
        return features.contains(feature);
    }

    public String blobColumn() {
        return blobColumn;
    }

    public PartitionStyle partitionStyle() {
        return partitionStyle;
    }

    public String partitionColumn() {
        return partitionColumn;
    }

    public enum Feature {
        BLOB,
        PARTITIONED
    }

    public enum PartitionStyle {
        HASH_INT,
        RANGE_INT,
        RANGE_DATE,
        LIST_STRING
    }
}
