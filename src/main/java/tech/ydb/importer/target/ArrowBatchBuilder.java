package tech.ydb.importer.target;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.table.query.arrow.ApacheArrowData;
import tech.ydb.table.query.arrow.ApacheArrowWriter;
import tech.ydb.table.values.StructType;

/**
 * Manages Apache Arrow resources (allocator, writer, batches) for columnar bulk upsert.
 * Type conversion is handled by {@link ValueReader} via {@link ArrowValueWriter}.
 */
public class ArrowBatchBuilder implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ArrowBatchBuilder.class);

    private final BufferAllocator allocator;
    private final ApacheArrowWriter writer;
    private final int maxBatchRows;

    public ArrowBatchBuilder(StructType paramType, int maxBatchRows) {
        this.allocator = new RootAllocator();
        this.maxBatchRows = maxBatchRows;

        ApacheArrowWriter.Schema schema = ApacheArrowWriter.newSchema();
        for (int i = 0; i < paramType.getMembersCount(); i++) {
            schema.addColumn(paramType.getMemberName(i), paramType.getMemberType(i));
        }
        this.writer = schema.createWriter(allocator);
    }

    public ApacheArrowWriter.Batch createBatch() {
        return writer.createNewBatch(maxBatchRows);
    }

    public ApacheArrowData buildBatch(ApacheArrowWriter.Batch batch) throws Exception {
        return batch.buildBatch();
    }

    @Override
    public void close() {
        try {
            writer.close();
        } catch (Exception e) {
            LOG.debug("Failed to close Arrow writer", e);
        }
        allocator.close();
    }
}
