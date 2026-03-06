package tech.ydb.importer.target;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.table.query.arrow.ApacheArrowData;
import tech.ydb.table.query.arrow.ApacheArrowWriter;
import tech.ydb.table.values.PrimitiveType;

/**
 * Arrow-based accumulator for BLOB data.
 * Reads BLOB from JDBC, splits into 64KB chunks, accumulates in Arrow batch,
 * and flushes to YDB supplemental table when threshold is reached.
 */
public class BlobAccumulator implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(BlobAccumulator.class);
    public static final int BLOCK_SIZE = 65536;

    private static final String COL_ID = "id";
    private static final String COL_POS = "pos";
    private static final String COL_VAL = "val";

    private final BufferAllocator allocator;
    private final ApacheArrowWriter writer;
    private final int maxBlobRows;
    private final YdbUpsertOp upsertOp;
    private final WriterPool writerPool;
    private final AtomicLong nextIdGen = new AtomicLong(0);
    private final boolean isBlob;

    private ApacheArrowWriter.Batch currentBatch;
    private int batchRowCount;

    public BlobAccumulator(String tablePath, TargetCP target, ProgressCounter progress,
                           int maxBlobRows, boolean isBlob, WriterPool writerPool) {
        this.allocator = new RootAllocator();
        this.maxBlobRows = Math.max(1, Math.min(maxBlobRows, 1000));
        this.isBlob = isBlob;
        this.writerPool = writerPool;

        this.upsertOp = new YdbUpsertOp(
                target.getRetryCtx(), tablePath,
                "blob arrow upsert issue for " + tablePath,
                progress::countBlobRows
        );

        ApacheArrowWriter.Schema schema = ApacheArrowWriter.newSchema();
        schema.addColumn(COL_ID, PrimitiveType.Int64);
        schema.addColumn(COL_POS, PrimitiveType.Int32);
        schema.addColumn(COL_VAL, PrimitiveType.Bytes);
        this.writer = schema.createWriter(allocator);
        this.currentBatch = writer.createNewBatch(maxBlobRows);
        this.batchRowCount = 0;
    }

    /**
     * Reads a BLOB from the ResultSet, splits into chunks, accumulates in Arrow batch.
     * Returns the generated BLOB id for the main table reference.
     *
     * @return BLOB id, or -1 if the value was NULL
     */
    public long writeBlob(SynthKey synthKey, ResultSet rs, int rsIndex) throws Exception {
        try (InputStream is = openStream(rs, rsIndex)) {
            if (rs.wasNull()) {
                if (synthKey != null) {
                    synthKey.updateSeparator();
                }
                return -1;
            }

            long id = nextIdGen.incrementAndGet();
            byte[] block = new byte[BLOCK_SIZE];
            int position = 0;

            while (true) {
                int bytesRead = is.read(block);
                if (bytesRead < 1) {
                    break;
                }

                ApacheArrowWriter.Row row = currentBatch.writeNextRow();
                row.writeInt64(COL_ID, id);
                row.writeInt32(COL_POS, position);
                if (bytesRead == block.length) {
                    row.writeBytes(COL_VAL, block);
                } else {
                    byte[] trimmed = new byte[bytesRead];
                    System.arraycopy(block, 0, trimmed, 0, bytesRead);
                    row.writeBytes(COL_VAL, trimmed);
                }
                position++;
                batchRowCount++;

                if (batchRowCount >= maxBlobRows) {
                    flush();
                }
            }

            if (synthKey != null) {
                ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
                buffer.putLong(id);
                synthKey.update(buffer);
            }

            return id;
        }
    }

    public void flush() throws Exception {
        if (batchRowCount == 0) {
            return;
        }
        ApacheArrowData data = currentBatch.buildBatch();
        int count = batchRowCount;
        if (writerPool != null) {
            writerPool.submit(new TaggedBatch(() -> upsertOp.upload(data, count)));
        } else {
            upsertOp.upload(data, count);
        }
        currentBatch = writer.createNewBatch(maxBlobRows);
        batchRowCount = 0;
    }

    private InputStream openStream(ResultSet rs, int index) throws Exception {
        if (isBlob) {
            return rs.getBlob(index).getBinaryStream();
        }
        return rs.getBinaryStream(index);
    }

    @Override
    public void close() {
        try {
            flush();
        } catch (Exception e) {
            LOG.warn("Failed to flush BlobAccumulator on close", e);
        }
        try {
            writer.close();
        } catch (Exception e) {
            LOG.debug("Failed to close Arrow writer", e);
        }
        allocator.close();
    }
}
