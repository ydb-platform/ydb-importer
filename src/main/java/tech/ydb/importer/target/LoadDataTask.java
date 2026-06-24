package tech.ydb.importer.target;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
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
import tech.ydb.table.query.BulkUpsertData;
import tech.ydb.table.query.arrow.ApacheArrowData;
import tech.ydb.table.query.arrow.ApacheArrowWriter;
import tech.ydb.table.values.ListType;
import tech.ydb.table.values.ListValue;
import tech.ydb.table.values.StructType;
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
    private final boolean useArrow;
    private final boolean useStringForClob;
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
                        && tab.getBlobTargets().isEmpty()
                        && tab.getClobTargets().isEmpty())
                ? owner.getConfig().getSource().getRetryCount()
                : 0;
        this.taskIdx = task.getIndex();
        this.taskBits = BlobReader.bitsRequired(tab.getMetadata().getTasks().size());
        this.partitionBuffers = tab.partitionBuffers();
        this.useArrow = owner.getConfig().getWorkers().isUseArrow();
        this.useStringForClob = owner.getTableLister().useStringForClobRead();
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
                    if (queries.size() > 1) {
                        LOG.info("Reading range {}", query.getName());
                    }
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
        final ResultSetMetaData rsmd = rs.getMetaData();
        final ColumnIndex[] columns = buildMainIndex(paramType, rsmd);
        final List<BlobReader> blobReaders = collectBlobReaders(columns);
        final List<ClobReader> clobReaders = collectClobReaders(columns);
        final SynthKey synthKey = tab.getTarget().hasSynthKey() ? new SynthKey() : null;

        PartitionBounds pb = partitionBuffers ? resolvePartitionBounds(rsmd) : null;
        if (partitionBuffers && pb == null) {
            LOG.debug("partition-buffers requested for {}.{} but no integer partition cuts, "
                    + "plain batching", tab.getSchema(), tab.getTable());
        }

        long copied = useArrow
                ? copyDataArrow(rs, paramType, columns, blobReaders, clobReaders, synthKey, pb)
                : copyDataRows(rs, paramType, columns, blobReaders, clobReaders, synthKey, pb);

        for (ColumnIndex ci : columns) {
            if (ci != null) {
                ci.getReader().flush();
            }
        }

        return copied;
    }

    /** Row batching, one buffer per partition when bounds are known, else a single buffer. */
    private long copyDataRows(ResultSet rs, StructType paramType, ColumnIndex[] columns,
            List<BlobReader> blobReaders, List<ClobReader> clobReaders, SynthKey synthKey,
            PartitionBounds pb) throws Exception {
        final ListType paramListType = ListType.of(paramType);
        final int partCount = (pb == null) ? 1 : pb.cuts.length + 1;
        final List<List<Value<?>>> buffers = new ArrayList<>(partCount);
        for (int i = 0; i < partCount; i++) {
            buffers.add(new ArrayList<>(maxBatchRows));
        }
        final RowValueWriter writer = new RowValueWriter(paramType);
        long copied = 0;
        long readStart = System.nanoTime();

        while (rs.next()) {
            rowIndex++;
            copied++;
            progress.countReadRows(1);
            setupBlobIds(blobReaders, clobReaders);

            int part = (pb == null) ? 0 : partitionOf(rs.getLong(pb.pkIndex), pb.cuts);
            Value<?>[] values = new Value[paramType.getMembersCount()];
            Arrays.fill(values, VoidValue.of());
            writer.setValues(values);
            readRow(rs, paramType, columns, writer, synthKey);
            List<Value<?>> buffer = buffers.get(part);
            buffer.add(paramType.newValueUnsafe(values));

            if (buffer.size() >= maxBatchRows) {
                progress.countReadBatch(System.nanoTime() - readStart);
                checkCancelled();
                submitRowBatch(paramListType, buffer);
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
                submitRowBatch(paramListType, buffer);
                buffer.clear();
            }
        }
        return copied;
    }

    /** Arrow batching, one batch per partition when bounds are known, else a single batch. */
    private long copyDataArrow(ResultSet rs, StructType paramType, ColumnIndex[] columns,
            List<BlobReader> blobReaders, List<ClobReader> clobReaders, SynthKey synthKey,
            PartitionBounds pb) throws Exception {
        final int partCount = (pb == null) ? 1 : pb.cuts.length + 1;
        final ArrowBatchBuilder[] builders = new ArrowBatchBuilder[partCount];
        final ApacheArrowWriter.Batch[] batches = new ApacheArrowWriter.Batch[partCount];
        final int[] counts = new int[partCount];
        final ArrowValueWriter writer = new ArrowValueWriter(paramType);
        long copied = 0;
        long readStart = System.nanoTime();
        try {
            for (int i = 0; i < partCount; i++) {
                builders[i] = new ArrowBatchBuilder(paramType, maxBatchRows);
            }
            while (rs.next()) {
                rowIndex++;
                copied++;
                progress.countReadRows(1);
                setupBlobIds(blobReaders, clobReaders);

                int part = (pb == null) ? 0 : partitionOf(rs.getLong(pb.pkIndex), pb.cuts);
                if (batches[part] == null) {
                    batches[part] = builders[part].newBatch();
                }
                writer.setRow(batches[part].writeNextRow());
                readRow(rs, paramType, columns, writer, synthKey);
                counts[part]++;

                if (counts[part] >= maxBatchRows) {
                    progress.countReadBatch(System.nanoTime() - readStart);
                    checkCancelled();
                    submitArrowBatch(batches[part], counts[part]);
                    batches[part] = null;
                    counts[part] = 0;
                    readStart = System.nanoTime();
                }
            }
            boolean counted = false;
            for (int i = 0; i < partCount; i++) {
                if (counts[i] > 0) {
                    if (!counted) {
                        progress.countReadBatch(System.nanoTime() - readStart);
                        counted = true;
                    }
                    checkCancelled();
                    submitArrowBatch(batches[i], counts[i]);
                }
            }
        } finally {
            for (ArrowBatchBuilder b : builders) {
                if (b != null) {
                    b.close();
                }
            }
        }
        return copied;
    }

    private void setupBlobIds(List<BlobReader> blobReaders, List<ClobReader> clobReaders) {
        if (blobReaders.isEmpty() && clobReaders.isEmpty()) {
            return;
        }
        long blobId = BlobReader.packBlobId(taskIdx, rowIndex, taskBits);
        for (BlobReader br : blobReaders) {
            br.setNextBlobId(blobId);
        }
        for (ClobReader cr : clobReaders) {
            cr.setNextClobId(blobId);
        }
    }

    private void submitRowBatch(ListType paramListType, List<Value<?>> batch) throws Exception {
        final ListValue lv = paramListType.newValue(batch);
        writerPool.submit(new UploadBatch(ydbOp, new BulkUpsertData(lv), batch.size(),
                () -> RowValueWriter.logValues(lv), tab));
    }

    private void submitArrowBatch(ApacheArrowWriter.Batch arrowBatch, int rowCount) throws Exception {
        final ApacheArrowData data = arrowBatch.buildBatch();
        writerPool.submit(new UploadBatch(ydbOp, data, rowCount,
                () -> ArrowValueWriter.logValues(data), tab));
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

    private static List<ClobReader> collectClobReaders(ColumnIndex[] columns) {
        List<ClobReader> readers = new ArrayList<>();
        for (ColumnIndex ci : columns) {
            if (ci != null && ci.getReader() instanceof ClobReader) {
                readers.add((ClobReader) ci.getReader());
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
            } else if (tab.getClobTargets().containsKey(columnName)) {
                TargetTable tt = tab.getClobTargets().get(columnName);
                String clobPath = target.getDatabase() + "/" + tt.getFullName();
                ValueReader reader = new ClobReader(clobPath, target.getRetryCtx(),
                        progress, maxBlobRows, useStringForClob);
                index[i] = new ColumnIndex(ixTarget, reader);
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
     * Reads the current source row column by column and writes the synthetic key column when configured.
     *
     * @param rs Source result set
     * @param type Target struct type
     * @param columns Column index mappings
     * @param writer Destination for the converted values
     * @param synthKey Row synthetic key, or null when unused
     * @throws Exception
     */
    private void readRow(ResultSet rs, StructType type, ColumnIndex[] columns,
            ValueWriter writer, SynthKey synthKey) throws Exception {
        for (int rsIdx = 1; rsIdx <= columns.length; rsIdx += 1) {
            ColumnIndex column = columns[rsIdx - 1];
            if (column == null) {
                continue;
            }

            int valuesIdx = column.getStructIndex();
            try {
                column.getReader().read(rs, rsIdx, valuesIdx, writer, synthKey);
            } catch (Exception ex) {
                throw new Exception("Failed conversion for column " + rsIdx + " " + type.getMemberName(valuesIdx), ex);
            }
        }

        if (synthKey != null) {
            writer.writeText(tab.getTarget().getSynthKeyPos(), synthKey.buildString());
        }
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
