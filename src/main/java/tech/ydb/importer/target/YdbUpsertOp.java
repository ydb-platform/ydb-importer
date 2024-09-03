package tech.ydb.importer.target;

import java.util.function.IntConsumer;

import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.settings.BulkUpsertSettings;
import tech.ydb.table.values.ListValue;

/**
 *
 * @author zinal
 */
public class YdbUpsertOp {

    private final SessionRetryContext retryCtx;
    private final String tablePath;
    private final String errorMsg;
    private final IntConsumer counter;
    private final BulkUpsertSettings upsertSettings = new BulkUpsertSettings();

    public YdbUpsertOp(SessionRetryContext retryCtx, String tablePath, String errorMsg, IntConsumer counter) {
        this.retryCtx = retryCtx;
        this.errorMsg = errorMsg;
        this.tablePath = tablePath;
        this.counter = counter;
    }

    public void upload(ListValue newValue) {
        if (newValue == null || newValue.isEmpty()) {
            return;
        }

        retryCtx.supplyStatus(
                session -> session.executeBulkUpsert(tablePath, newValue, upsertSettings)
        ).join().expectSuccess(errorMsg);

        counter.accept(newValue.size());
    }
}
