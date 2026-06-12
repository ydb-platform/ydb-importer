package tech.ydb.importer.integration.verification;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongFunction;

import tech.ydb.importer.integration.verification.TableScenario.PartitionStyle;

/** Fluent builder for {@link TableScenario} */
public final class Scenario {

    private Scenario() {
    }

    public static Builder table(String name, long rowCount) {
        return new Builder(name, rowCount);
    }

    public static final class Builder {
        private final String tableName;
        private final long rowCount;
        private String keyColumn = "id";
        private final List<ColumnDef> defs = new ArrayList<>();
        private String blobColumn;
        private LongFunction<byte[]> blobOracle;
        private PartitionStyle partitionStyle;
        private String partitionColumn;

        private Builder(String tableName, long rowCount) {
            this.tableName = tableName;
            this.rowCount = rowCount;
        }

        public Builder key(String column) {
            this.keyColumn = column;
            return this;
        }

        public Builder col(String name, LogicalType type, LongFunction<Object> fn) {
            defs.add(new ColumnDef(name, type, fn, false));
            return this;
        }

        public Builder colNullable(String name, LogicalType type, LongFunction<Object> fn) {
            defs.add(new ColumnDef(name, type, fn, true));
            return this;
        }

        public Builder partition(PartitionStyle style, String column) {
            this.partitionStyle = style;
            this.partitionColumn = column;
            return this;
        }

        public Builder blob(String column, LongFunction<byte[]> oracle) {
            this.blobColumn = column;
            this.blobOracle = oracle;
            return this;
        }

        public TableScenario build() {
            List<ColumnSpec> columns = new ArrayList<>(defs.size());
            for (ColumnDef d : defs) {
                columns.add(new ColumnSpec(d.name, d.type, d.nullable));
            }
            RowOracle oracle = new RowOracle() {
                @Override
                public long rowCount() {
                    return rowCount;
                }

                @Override
                public Map<String, Object> expectedFor(long id) {
                    Map<String, Object> row = new HashMap<>(defs.size());
                    for (ColumnDef d : defs) {
                        row.put(d.name, d.fn.apply(id));
                    }
                    return row;
                }

                @Override
                public byte[] expectedBlobFor(long id) {
                    return blobOracle == null ? null : blobOracle.apply(id);
                }
            };
            return new TableScenario(tableName, keyColumn, columns, oracle,
                    blobColumn, partitionStyle, partitionColumn);
        }
    }

    private static final class ColumnDef {
        final String name;
        final LogicalType type;
        final LongFunction<Object> fn;
        final boolean nullable;

        ColumnDef(String name, LogicalType type, LongFunction<Object> fn,
                  boolean nullable) {
            this.name = name;
            this.type = type;
            this.fn = fn;
            this.nullable = nullable;
        }
    }
}
