package tech.ydb.importer.target;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import tech.ydb.table.query.arrow.ApacheArrowWriter;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.Type;

/**
 * Arrow batch builder for bulk upserts.
 */
public class ArrowBatchBuilder implements AutoCloseable {

    private final int maxBatchRows;
    private final BufferAllocator allocator;
    private final ApacheArrowWriter writer;

    public ArrowBatchBuilder(StructType paramType, int maxBatchRows) {
        this.maxBatchRows = maxBatchRows;
        this.allocator = new RootAllocator();
        ApacheArrowWriter.Schema schema = ApacheArrowWriter.newSchema();
        for (int i = 0; i < paramType.getMembersCount(); i++) {
            Type memberType = paramType.getMemberType(i);
            if (memberType.getKind() == Type.Kind.OPTIONAL) {
                schema.addNullableColumn(paramType.getMemberName(i), memberType.unwrapOptional());
            } else {
                schema.addColumn(paramType.getMemberName(i), memberType);
            }
        }
        this.writer = schema.createWriter(allocator);
    }

    public ApacheArrowWriter.Batch newBatch() {
        return writer.createNewBatch(maxBatchRows);
    }

    @Override
    public void close() {
        writer.close();
        allocator.close();
    }
}
