package tech.ydb.importer.target;

import tech.ydb.table.values.ListValue;

/**
 * Batch of rows paired with the target upsert operation.
 */
public class TaggedBatch {

    static final TaggedBatch POISON = new TaggedBatch(null, null);

    private final YdbUpsertOp op;
    private final ListValue batch;

    public TaggedBatch(YdbUpsertOp op, ListValue batch) {
        this.op = op;
        this.batch = batch;
    }

    public YdbUpsertOp getOp() {
        return op;
    }

    public ListValue getBatch() {
        return batch;
    }
}
