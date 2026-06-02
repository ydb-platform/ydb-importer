package tech.ydb.importer.target;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
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
import tech.ydb.importer.source.TaskInfo;
import tech.ydb.importer.source.TaskQuery;
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
    private final TaskInfo task;
    private final ProgressCounter progress;

    private final int maxBatchRows;
    private final int maxBlobRows;
    private final int fetchSize;
    private final int retryCount;
    private final int taskIdx;
    private final int taskBits;
    private final boolean partitionBuffers;
    private final WriterPool writerPool;
    private long rowIndex;

    private static final long INITIAL_BACKOFF_MS = 1000;
    private static final long MAX_BACKOFF_MS = 30_000;

    public LoadDataTask(YdbImporter owner, ProgressCounter progress, TableDecision tab,
            TaskInfo task, WriterPool writerPool) {
        this.source = owner.getSourceCP();
        this.target = owner.getTargetCP();

        this.ydbOp = new YdbUpsertOp(
                target.getRetryCtx(),
                target.getDatabase() + "/" + tab.getTarget().getFullName(),
                "failed upsert to " + tab.getTarget().getFullName(),
                progress::countWrittenRows
        );
        this.tab = tab;
        this.task = task;
        this.progress = progress;
        this.maxBatchRows = owner.getConfig().getTarget().getMaxBatchRows();
        this.maxBlobRows = owner.getConfig().getTarget().getMaxBlobRows();
        this.fetchSize = owner.getConfig().getSource().getFetchSize();
        this.retryCount = (tab.getMetadata().getTasks().size() > 1
                        && tab.getBlobTargets().isEmpty())
                ? owner.getConfig().getSource().getRetryCount()
                : 0;
        this.taskIdx = task.getIndex();
        this.taskBits = BlobReader.bitsRequired(tab.getMetadata().getTasks().size());
        this.partitionBuffers = tab.partitionBuffers();
        this.writerPool = writerPool;
        this.rowIndex = 0;
    }

    @Override
    public Boolean call() throws Exception {
        if (!tab.isValid()) {
            LOG.warn("Skipping incomplete source table {}.{}", tab.getSchema(), tab.getTable());
            return false;
        }
        if (tab.isFailure()) {
            LOG.warn("Skipping {} because the table has already failed", task.getName());
            return false;
        }
        LOG.info("Loading data from {}", task.getName());
        try {
            long copied = executeTask();
            LOG.info("Copied {} rows from {}", copied, task.getName());
            return true;
        } catch (Throwable e) {
            if (e instanceof InterruptedException) {
                LOG.warn("Interrupted {}", task.getName());
                return false;
            }
            if (tab.isFailure()) {
                LOG.warn("Cancelled {} because the table has already failed", task.getName());
            } else {
                LOG.error("Failed to load data from {}", task.getName(), e);
                tab.setFailure(true);
            }
            return false;
        }
    }

    private void checkCancelled() {
        if (tab.isFailure()) {
            throw new RuntimeException("Cancelled: table " + tab.getSchema() + "."
                    + tab.getTable() + " marked as failed by another task");
        }
    }

    /**
     * Reads all queries of a task, retrying each one on failure.
     */
    private long executeTask() throws Exception {
        List<TaskQuery> queries = task.getQueries();
        long copied = 0;
        int nextQuery = 0;
        int attempt = 0;
        long backoffMs = INITIAL_BACKOFF_MS;
        long savedRowIndex = rowIndex;

        while (nextQuery < queries.size()) {
            try (Connection con = source.getConnection()) {
                while (nextQuery < queries.size()) {
                    checkCancelled();
                    savedRowIndex = rowIndex;
                    TaskQuery query = queries.get(nextQuery);
                    copied += executeQuery(con, query.getSql());
                    nextQuery++;
                    attempt = 0;
                    backoffMs = INITIAL_BACKOFF_MS;
                }
            } catch (SQLException e) {
                if (tab.isFailure()) {
                    throw e;
                }
                rowIndex = savedRowIndex;
                if (++attempt > retryCount) {
                    throw e;
                }
                TaskQuery failed = queries.get(nextQuery);
                LOG.warn("Query {} failed (attempt {}/{}), retrying in {} ms",
                        failed.getName(), attempt, retryCount, backoffMs, e);
                Thread.sleep(backoffMs);
                backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
            }
        }
        return copied;
    }

    private long executeQuery(Connection con, String sql) throws Exception {
        long copied;
        try (PreparedStatement ps = con.prepareStatement(sql)) {
            ps.setFetchSize(fetchSize);
            try (ResultSet rs = ps.executeQuery()) {
                copied = copyData(rs);
            }
        }
        if (!con.getAutoCommit()) {
            con.commit();
        }
        return copied;
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
        final List<BlobReader> blobReaders = collectBlobReaders(columns);
        final SynthKey synchKey = tab.getTarget().hasSynthKey() ? new SynthKey() : null;

        PartitionBounds pb = partitionBuffers ? resolvePartitionBounds(rsmd) : null;
        if (partitionBuffers && pb == null) {
            LOG.debug("partition-buffers requested for {}.{} but no integer partition cuts, "
                    + "plain batching", tab.getSchema(), tab.getTable());
        }
        long copied = (pb != null)
                ? copyDataGrouped(rs, paramType, paramListType, columns, blobReaders, synchKey, pb)
                : copyDataPlain(rs, paramType, paramListType, columns, blobReaders, synchKey);

        for (ColumnIndex ci : columns) {
            if (ci != null) {
                ci.getReader().flush();
            }
        }

        return copied;
    }

    /** Plain batching: fills one batch in read order, no regrouping.
     *  A batch may span several YDB partitions. */
    private long copyDataPlain(ResultSet rs, StructType paramType, ListType paramListType,
            ColumnIndex[] columns, List<BlobReader> blobReaders, SynthKey synthKey)
            throws Exception {
        final List<Value<?>> batch = new ArrayList<>(maxBatchRows);
        long copied = 0;
        long readStart = System.nanoTime();
        while (rs.next()) {
            rowIndex++;
            copied++;
            progress.countReadRows(1);
            setNextBlobIds(blobReaders);
            batch.add(read(rs, paramType, columns, synthKey));
            if (batch.size() >= maxBatchRows) {
                progress.countReadBatch(System.nanoTime() - readStart);
                checkCancelled();
                writerPool.submit(new UploadBatch(ydbOp, paramListType.newValue(batch)));
                batch.clear();
                readStart = System.nanoTime();
            }
        }
        if (!batch.isEmpty()) {
            progress.countReadBatch(System.nanoTime() - readStart);
            checkCancelled();
            writerPool.submit(new UploadBatch(ydbOp, paramListType.newValue(batch)));
            batch.clear();
        }
        return copied;
    }

    /** Regroups rows into YDB partition buffers so each batch goes to a single YDB partition. */
    private long copyDataGrouped(ResultSet rs, StructType paramType, ListType paramListType,
            ColumnIndex[] columns, List<BlobReader> blobReaders, SynthKey synthKey,
            PartitionBounds pb) throws Exception {
        final int partCount = pb.cuts.length + 1;
        final List<List<Value<?>>> buffers = new ArrayList<>(partCount);
        for (int i = 0; i < partCount; i++) {
            buffers.add(new ArrayList<>(maxBatchRows));
        }
        long copied = 0;
        long readStart = System.nanoTime();
        while (rs.next()) {
            rowIndex++;
            copied++;
            progress.countReadRows(1);
            setNextBlobIds(blobReaders);
            long key = rs.getLong(pb.pkIndex);
            List<Value<?>> buffer = buffers.get(partitionOf(key, pb.cuts));
            buffer.add(read(rs, paramType, columns, synthKey));
            if (buffer.size() >= maxBatchRows) {
                progress.countReadBatch(System.nanoTime() - readStart);
                checkCancelled();
                writerPool.submit(new UploadBatch(ydbOp, paramListType.newValue(buffer)));
                buffer.clear();
                readStart = System.nanoTime();
            }
        }
        boolean counted = false;
        for (List<Value<?>> buffer : buffers) {
            if (!buffer.isEmpty()) {
                if (!counted) {
                    progress.countReadBatch(System.nanoTime() - readStart);
                    counted = true;
                }
                checkCancelled();
                writerPool.submit(new UploadBatch(ydbOp, paramListType.newValue(buffer)));
                buffer.clear();
            }
        }
        return copied;
    }

    private void setNextBlobIds(List<BlobReader> blobReaders) {
        if (!blobReaders.isEmpty()) {
            long blobId = BlobReader.packBlobId(taskIdx, rowIndex, taskBits);
            for (BlobReader br : blobReaders) {
                br.setNextBlobId(blobId);
            }
        }
    }

    /** Which YDB partition a key falls into, by binary search over the cuts. */
    private static int partitionOf(long key, long[] cuts) {
        int lo = 0;
        int hi = cuts.length;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (key >= cuts[mid]) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    /** Resolves the leading-PK column index and integer partition cuts.
     *  Returns null when regrouping is not possible. */
    private PartitionBounds resolvePartitionBounds(ResultSetMetaData rsmd) throws SQLException {
        List<ColumnInfo> key = tab.getMetadata().getKey();
        List<String> cutStrings = tab.getMetadata().getYdbPartitioning().getCuts();
        if (key.isEmpty() || cutStrings.isEmpty()) {
            return null;
        }
        ColumnInfo pk = key.get(0);
        if (!YdbTypeMapper.partitionableInteger(pk, tab.getOptions())) {
            return null;
        }
        long[] cuts = new long[cutStrings.size()];
        try {
            for (int i = 0; i < cuts.length; i++) {
                cuts[i] = Long.parseLong(cutStrings.get(i).trim());
            }
        } catch (NumberFormatException ex) {
            return null;
        }
        int pkIndex = -1;
        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
            if (pk.getName().equalsIgnoreCase(rsmd.getColumnName(i))) {
                pkIndex = i;
                break;
            }
        }
        if (pkIndex < 0) {
            return null;
        }
        return new PartitionBounds(cuts, pkIndex);
    }

    private static final class PartitionBounds {
        private final long[] cuts;
        private final int pkIndex;

        PartitionBounds(long[] cuts, int pkIndex) {
            this.cuts = cuts;
            this.pkIndex = pkIndex;
        }
    }

    private static List<BlobReader> collectBlobReaders(ColumnIndex[] columns) {
        List<BlobReader> readers = new ArrayList<>();
        for (ColumnIndex ci : columns) {
            if (ci != null && ci.getReader() instanceof BlobReader) {
                readers.add((BlobReader) ci.getReader());
            }
        }
        return readers;
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
                    ValueReader reader = new BlobReader(blobPath, target.getRetryCtx(), progress, maxBlobRows, isBlob);
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
