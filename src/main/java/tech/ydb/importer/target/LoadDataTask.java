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
import tech.ydb.importer.source.ChunkInfo;
import tech.ydb.importer.source.ColumnInfo;
import tech.ydb.importer.source.PartitionInfo;
import tech.ydb.importer.source.SourceCP;
import tech.ydb.table.query.arrow.ApacheArrowData;
import tech.ydb.table.query.arrow.ApacheArrowWriter;
import tech.ydb.table.values.ListType;
import tech.ydb.table.values.ListValue;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.Value;

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
    private final PartitionInfo partition;
    private final ProgressCounter progress;

    private final int maxBatchRows;
    private final int maxBlobRows;
    private final int fetchSize;
    private final int retryCount;
    private final WriterPool writerPool;
    private final boolean useArrow;
    private final int partitionIdx;
    private long rowIndex;

    private static final long INITIAL_BACKOFF_MS = 1000;

    public LoadDataTask(YdbImporter owner, ProgressCounter progress, TableDecision tab,
            WriterPool writerPool) {
        this(owner, progress, tab, null, writerPool);
    }

    public LoadDataTask(YdbImporter owner, ProgressCounter progress, TableDecision tab,
            PartitionInfo partition, WriterPool writerPool) {
        this.source = owner.getSourceCP();
        this.target = owner.getTargetCP();

        this.ydbOp = new YdbUpsertOp(
                target.getRetryCtx(),
                target.getDatabase() + "/" + tab.getTarget().getFullName(),
                "failed upsert to " + tab.getTarget().getFullName(),
                progress::countWrittenRows
        );
        this.tab = tab;
        this.partition = partition;
        this.progress = progress;
        this.maxBatchRows = owner.getConfig().getTarget().getMaxBatchRows();
        this.maxBlobRows = owner.getConfig().getTarget().getMaxBlobRows();
        this.fetchSize = owner.getConfig().getSource().getFetchSize();
        this.retryCount = owner.getConfig().getSource().getRetryCount();
        this.writerPool = writerPool;
        this.useArrow = owner.getConfig().getWorkers().isUseArrow();
        this.partitionIdx = (partition != null) ? partition.getIndex() : 0;
        this.rowIndex = 0;
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
        try {
            long copied;
            if (partition != null) {
                copied = executePartition();
                LOG.info("Copied {} rows from partition {}", copied, partition.getName());
            } else {
                LOG.info("Loading data from source table {}.{}", tab.getSchema(), tab.getTable());
                try (Connection con = source.getConnection()) {
                    copied = executeQuery(con, tab.getMetadata().getBasicSql(), null);
                }
                LOG.info("Copied {} rows from source table {}.{}",
                        copied, tab.getSchema(), tab.getTable());
            }
            return true;
        } catch (Throwable e) {
            LOG.error("Failed to load data from {}", getLabel(), e);
            tab.setFailure(true);
            return false;
        }
    }

    private long nextBlobId() {
        rowIndex++;
        return BlobReader.packBlobId(partitionIdx, rowIndex);
    }

    private String getLabel() {
        if (partition != null) {
            return partition.getName();
        }
        return tab.getSchema() + "." + tab.getTable();
    }

    /**
     * Reads all chunks of a partition with connection-level retry.
     */
    private long executePartition() throws Exception {
        LOG.info("Loading partition {}", partition.getName());
        List<ChunkInfo> chunks = partition.getChunks();
        long copied = 0;
        int nextChunk = 0;
        int attempt = 0;
        int failingChunk = -1;
        long backoffMs = INITIAL_BACKOFF_MS;
        long savedRowIndex = rowIndex;

        while (nextChunk < chunks.size()) {
            try (Connection con = source.getConnection()) {
                while (nextChunk < chunks.size()) {
                    savedRowIndex = rowIndex;
                    ChunkInfo chunk = chunks.get(nextChunk);
                    copied += executeQuery(con, chunk.getQuerySql(), chunk.getName());
                    nextChunk++;
                    attempt = 0;
                    backoffMs = INITIAL_BACKOFF_MS;
                    failingChunk = -1;
                }
            } catch (Exception e) {
                rowIndex = savedRowIndex;
                if (nextChunk != failingChunk) {
                    attempt = 0;
                    backoffMs = INITIAL_BACKOFF_MS;
                    failingChunk = nextChunk;
                }
                if (++attempt > retryCount) {
                    throw e;
                }
                ChunkInfo failed = chunks.get(nextChunk);
                LOG.warn("Chunk {} failed (attempt {}/{}), retrying in {} ms",
                        failed.getName(), attempt, retryCount, backoffMs, e);
                Thread.sleep(backoffMs);
                backoffMs *= 2;
            }
        }
        return copied;
    }

    private long executeQuery(Connection con, String sql, String partitionLabel) throws Exception {
        if (partitionLabel != null) {
            LOG.info("Reading partition {} of {}.{}", partitionLabel, tab.getSchema(), tab.getTable());
        }
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

    private long copyData(ResultSet rs) throws Exception {
        final StructType paramType = tab.getTarget().getFields();
        final ResultSetMetaData rsmd = rs.getMetaData();
        final ColumnIndex[] columns = buildMainIndex(paramType, rsmd);
        final SynthKey synthKey = tab.getTarget().hasSynthKey() ? new SynthKey() : null;
        final int synthKeyPos = tab.getTarget().getSynthKeyPos();

        long copied;
        if (useArrow) {
            copied = copyDataArrow(rs, paramType, columns, synthKey, synthKeyPos);
        } else {
            copied = copyDataRows(rs, paramType, columns, synthKey, synthKeyPos);
        }

        flushReaders(columns);
        return copied;
    }

    private long copyDataRows(ResultSet rs, StructType paramType, ColumnIndex[] columns,
                              SynthKey synthKey, int synthKeyPos) throws Exception {
        final ListType paramListType = ListType.of(paramType);
        final List<Value<?>> batch = new ArrayList<>(maxBatchRows);
        long copied = 0;

        while (rs.next()) {
            copied++;
            progress.countReadRows(1);

            RowValueWriter writer = new RowValueWriter(paramType.getMembersCount());
            writeRow(rs, columns, writer, synthKey);
            if (synthKey != null) {
                writer.writeText(synthKeyPos, synthKey.buildString());
            }
            batch.add(writer.toStructValue(paramType));

            if (batch.size() >= maxBatchRows) {
                sendRowBatch(paramListType, batch);
                batch.clear();
            }
        }

        if (!batch.isEmpty()) {
            sendRowBatch(paramListType, batch);
            batch.clear();
        }

        return copied;
    }

    private long copyDataArrow(ResultSet rs, StructType paramType, ColumnIndex[] columns,
                               SynthKey synthKey, int synthKeyPos) throws Exception {
        long copied = 0;

        try (ArrowBatchBuilder arrowBuilder = new ArrowBatchBuilder(paramType, maxBatchRows)) {
            ApacheArrowWriter.Batch batch = arrowBuilder.createBatch();
            int batchRowCount = 0;

            while (rs.next()) {
                copied++;
                progress.countReadRows(1);

                ApacheArrowWriter.Row row = batch.writeNextRow();
                ArrowValueWriter writer = new ArrowValueWriter(row, paramType);
                writeRow(rs, columns, writer, synthKey);
                if (synthKey != null) {
                    writer.writeText(synthKeyPos, synthKey.buildString());
                }
                batchRowCount++;

                if (batchRowCount >= maxBatchRows) {
                    sendArrowBatch(arrowBuilder, batch, batchRowCount);
                    batch = arrowBuilder.createBatch();
                    batchRowCount = 0;
                }
            }

            if (batchRowCount > 0) {
                sendArrowBatch(arrowBuilder, batch, batchRowCount);
            }
        }

        return copied;
    }

    /**
     * Writes a single source row through the given ValueWriter.
     * Shared between row-based and Arrow-based paths.
     */
    private void writeRow(ResultSet rs, ColumnIndex[] columns,
                          ValueWriter writer, SynthKey synthKey) throws Exception {
        long blobId = nextBlobId();

        for (int rsIdx = 1; rsIdx <= columns.length; rsIdx++) {
            ColumnIndex column = columns[rsIdx - 1];
            if (column == null) {
                continue;
            }

            ValueReader reader = column.getReader();
            if (reader instanceof BlobReader) {
                ((BlobReader) reader).setNextBlobId(blobId);
            } else if (reader instanceof ClobReader) {
                ((ClobReader) reader).setNextClobId(blobId);
            }

            try {
                reader.read(rs, rsIdx, column.getStructIndex(), writer, synthKey);
            } catch (Exception ex) {
                StructType paramType = tab.getTarget().getFields();
                throw new Exception("Failed conversion for column " + rsIdx
                        + " " + paramType.getMemberName(column.getStructIndex()), ex);
            }
        }
    }

    private void sendRowBatch(ListType paramListType, List<Value<?>> batch) throws Exception {
        ListValue values = paramListType.newValue(batch);
        writerPool.submit(new TaggedBatch(() -> ydbOp.upload(values)));
    }

    private void sendArrowBatch(ArrowBatchBuilder arrowBuilder, ApacheArrowWriter.Batch batch,
                                int rowCount) throws Exception {
        ApacheArrowData data = arrowBuilder.buildBatch(batch);
        writerPool.submit(new TaggedBatch(() -> ydbOp.upload(data, rowCount)));
    }

    private void flushReaders(ColumnIndex[] columns) throws Exception {
        for (ColumnIndex ci : columns) {
            if (ci != null) {
                ci.getReader().flush();
            }
        }
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
                    boolean isBlobObj = ci.isBlobAsObject();
                    ValueReader reader = new BlobReader(blobPath, target.getRetryCtx(),
                            progress, maxBlobRows, isBlobObj);
                    index[i] = new ColumnIndex(ixTarget, reader);
                }
            } else if (ColumnInfo.isClob(ci.getSqlType())) {
                TargetTable tt = tab.getBlobTargets().get(columnName);
                if (tt == null) {
                    LOG.warn("Missing aux target table for CLOB column {} "
                            + "of source {}.{}", columnName, tab.getSchema(), tab.getTable());
                } else {
                    String clobPath = target.getDatabase() + "/" + tt.getFullName();
                    ValueReader reader = new ClobReader(clobPath, target.getRetryCtx(),
                            progress, maxBlobRows);
                    index[i] = new ColumnIndex(ixTarget, reader);
                }
            } else {
                ValueReader reader = ValueReader.getReader(
                        paramListType.getMemberType(ixTarget), ci.getSqlType());
                if (reader == null) {
                    LOG.warn("Unsupported BLOB/CLOB type for column {} (SQL type {}) "
                            + "of source {}.{} - column SKIPPED",
                            columnName, ci.getSqlType(), tab.getSchema(), tab.getTable());
                } else {
                    index[i] = new ColumnIndex(ixTarget, reader);
                }
            }
        }
        return index;
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
