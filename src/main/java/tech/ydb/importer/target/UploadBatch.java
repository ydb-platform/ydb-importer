package tech.ydb.importer.target;

import tech.ydb.table.values.ListValue;

/**
 * Batch of rows paired with the target upsert operation.
 */
public class UploadBatch {

    static final UploadBatch SHUTDOWN_SIGNAL = new UploadBatch(null, null);

    private final YdbUpsertOp op;
    private final ListValue batch;

    public UploadBatch(YdbUpsertOp op, ListValue batch) {
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
