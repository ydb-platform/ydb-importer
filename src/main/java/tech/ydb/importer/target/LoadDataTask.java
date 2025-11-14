package tech.ydb.importer.target;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.importer.TableDecision;
import tech.ydb.importer.YdbImporter;
import tech.ydb.importer.source.ColumnInfo;
import tech.ydb.importer.source.SourceCP;
import tech.ydb.table.values.ListType;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.StructValue;
import tech.ydb.table.values.Value;
import tech.ydb.table.values.VoidValue;

/**
 *
 * @author zinal
 */
public class LoadDataTask implements Callable<Boolean> {

    private static final Logger LOG = LoggerFactory.getLogger(LoadDataTask.class);

    private final SourceCP source;
    private final TargetCP target;

    private final YdbUpsertOp ydbOp;
    private final TableDecision tab;
    private final ProgressCounter progress;

    private final int maxBatchRows;

    public LoadDataTask(YdbImporter owner, ProgressCounter progress, TableDecision tab) {
        this.source = owner.getSourceCP();
        this.target = owner.getTargetCP();

        this.ydbOp = new YdbUpsertOp(
                target.getRetryCtx(),
                target.getDatabase() + "/" + tab.getTarget().getFullName(),
                "failed upsert to " + tab.getTarget().getFullName(),
                progress::countWrittenRows
        );
        this.tab = tab;
        this.progress = progress;
        this.maxBatchRows = owner.getConfig().getTarget().getMaxBatchRows();
    }

    @Override
    public Boolean call() throws Exception {
        if (!tab.isValid()) {
            LOG.warn("Skipping incomplete source table {}.{}", tab.getSchema(), tab.getTable());
            return false;
        }
        if (tab.isFailure()) {
            LOG.warn("Skipping failed source table {}.{}", tab.getSchema(), tab.getTable());
            return false;
        }
        LOG.info("Loading data from source table {}.{}", tab.getSchema(), tab.getTable());
        try (Connection con = source.getConnection()) {
            try (PreparedStatement ps = con.prepareStatement(tab.getMetadata().getBasicSql())) {
                try (ResultSet rs = ps.executeQuery()) {
                    long copied = copyData(rs);
                    LOG.info("Copied {} rows from source table {}.{}", copied, tab.getSchema(), tab.getTable());
                    return true;
                }
            }
        } catch (Throwable e) {
            LOG.error("Failed to load data from table {}.{}", tab.getSchema(), tab.getTable(), e);
            tab.setFailure(true);
            return false;
        }
    }

    /**
     * Reads the source ResultSet rows and upserts the data to YDB tables.
     *
     * @param rs Input result set
     * @throws Exception
     */
    private long copyData(ResultSet rs) throws Exception {
        final StructType paramType = tab.getTarget().getFields();
        final ListType paramListType = ListType.of(paramType);
        final ResultSetMetaData rsmd = rs.getMetaData();
        final ColumnIndex[] columns = buildMainIndex(paramType, rsmd);

        final List<Value<?>> batch = new ArrayList<>(maxBatchRows);
        final SynthKey synchKey = tab.getTarget().hasSynthKey() ? new SynthKey() : null;

        long copied = 0;

        while (rs.next()) {
            copied++;
            progress.countReadRows(1);

            batch.add(read(rs, paramType, columns, synchKey));
            if (batch.size() >= maxBatchRows) {
                ydbOp.upload(paramListType.newValue(batch));
                batch.clear();
            }
        }

        if (!batch.isEmpty()) {
            ydbOp.upload(paramListType.newValue(batch));
            batch.clear();
        }

        // Flush all readers
        for (ColumnIndex ci : columns) {
            if (ci != null) {
                ci.getReader().flush();
            }
        }

        return copied;
    }

    private ColumnIndex[] buildMainIndex(StructType paramListType, ResultSetMetaData rsmd) throws Exception {
        final Map<String, Integer> targetColumns = new HashMap<>();
        for (int i = 0; i < paramListType.getMembersCount(); ++i) {
            String memberName = paramListType.getMemberName(i);
            targetColumns.put(memberName, i);
        }

        ColumnIndex[] index = new ColumnIndex[rsmd.getColumnCount()];
        for (int i = 0; i < rsmd.getColumnCount(); i++) {
            String columnName = rsmd.getColumnName(i + 1);
            ColumnInfo ci = tab.getMetadata().findColumn(columnName);
            if (ci == null) {
                LOG.warn("Unexpected column {} in the source table {}.{} - column SKIPPED",
                        columnName, tab.getSchema(), tab.getTable());
                continue;
            }

            Integer ixTarget = targetColumns.get(ci.getDestinationName());
            if (ixTarget == null) {
                LOG.warn("Unexpected struct member {} in the source table {}.{} - column SKIPPED",
                        ci.getDestinationName(), tab.getSchema(), tab.getTable());
                continue;
            }

            // Blob checking has to be done based on the ColumnInfo declared type.
            // The reason for this is PostgreSQL ugly approach, where the driver
            // does not return BLOB type even for "lo" typed columns.
            if (ColumnInfo.isBlob(ci.getSqlType())) {
                // We need the full path of the BLOB storage table.
                TargetTable tt = tab.getBlobTargets().get(columnName);
                if (tt == null) {
                    LOG.warn("Missing aux target table for BLOB column {} "
                            + "of source {}.{}", columnName, tab.getSchema(), tab.getTable());
                } else {
                    String blobPath = target.getDatabase() + "/" + tt.getFullName();
                    boolean isBlob = ci.isBlobAsObject();
                    ValueReader reader = new BlobReader(blobPath, target.getRetryCtx(), progress, maxBatchRows, isBlob);
                    index[i] = new ColumnIndex(ixTarget, reader);
                }
            } else {
                ValueReader reader = ValueReader.getReader(paramListType.getMemberType(ixTarget), ci.getSqlType());
                if (reader == null) {
                    LOG.warn("Missing aux target table for BLOB column {} "
                            + "of source {}.{}", columnName, tab.getSchema(), tab.getTable());
                } else {
                    index[i] = new ColumnIndex(ixTarget, reader);
                }
            }
        }
        return index;
    }

    /**
     * Converts the current row from the source ResultSet to the StructValue
     * representation.
     *
     * @param type StructValue type definition
     * @param rs Input result set
     * @return StructValue with the converted copies of fields
     * @throws Exception
     */
    private StructValue read(ResultSet rs, StructType type, ColumnIndex[] columns, SynthKey synthKey) throws Exception {
        Value<?>[] values = new Value[type.getMembersCount()];
        for (int idx = 0; idx < values.length; idx += 1) {
            values[idx] = VoidValue.of();
        }

        for (int rsIdx = 1; rsIdx <= columns.length; rsIdx += 1) {
            ColumnIndex column = columns[rsIdx - 1];
            if (column == null) {
                continue;
            }

            int valuesIdx = column.getStructIndex();
            try {
                values[valuesIdx] = column.getReader().readValue(synthKey, rs, rsIdx);
            } catch (Exception ex) {
                throw new Exception("Failed conversion for column " + rsIdx + " " + type.getMemberName(valuesIdx), ex);
            }
        }

        if (synthKey != null) {
            values[tab.getTarget().getSynthKeyPos()] = synthKey.build();
        }
        return type.newValueUnsafe(values);
    }

    private static class ColumnIndex {

        private final int structIndex;
        private final ValueReader reader;

        ColumnIndex(int structIndex, ValueReader reader) {
            this.structIndex = structIndex;
            this.reader = reader;
        }

        public int getStructIndex() {
            return this.structIndex;
        }

        public ValueReader getReader() {
            return reader;
        }
    }
}
