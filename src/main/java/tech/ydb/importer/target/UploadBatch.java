package tech.ydb.importer.target;

import tech.ydb.importer.TableDecision;
import tech.ydb.table.query.BulkUpsertData;

/**
 * Batch of rows paired with the target upsert operation.
 */
public class UploadBatch {

    static final UploadBatch SHUTDOWN_SIGNAL = new UploadBatch(null, null, 0, null, null);

    private final YdbUpsertOp op;
    private final BulkUpsertData data;
    private final int rowCount;
    private final Runnable onFailure;
    private final TableDecision tab;

    public UploadBatch(YdbUpsertOp op, BulkUpsertData data, int rowCount, Runnable onFailure,
            TableDecision tab) {
        this.op = op;
        this.data = data;
        this.rowCount = rowCount;
        this.onFailure = onFailure;
        this.tab = tab;
    }

    public void markFailed() {
        if (tab != null) {
            tab.setFailure(true);
        }
    }

    public String label() {
        return tab == null ? "?" : tab.getSchema() + "." + tab.getTable();
    }

    public YdbUpsertOp getOp() {
        return op;
    }

    public BulkUpsertData getData() {
        return data;
    }

    public int getRowCount() {
        return rowCount;
    }

    public Runnable getOnFailure() {
        return onFailure;
    }
}
