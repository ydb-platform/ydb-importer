package tech.ydb.importer.target;

import java.util.function.IntConsumer;

import tech.ydb.core.Status;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.query.BulkUpsertData;
import tech.ydb.table.settings.BulkUpsertSettings;

/**
 *
 * @author zinal
 */
public class YdbUpsertOp {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(YdbUpsertOp.class);

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

    public void upload(BulkUpsertData data, int rowCount, Runnable onFailure) {
        if (data == null || rowCount == 0) {
            return;
        }

        Status status = retryCtx.supplyStatus(
                session -> session.executeBulkUpsert(tablePath, data, upsertSettings)
        ).join();

        if (status.isSuccess()) {
            counter.accept(rowCount);
            return;
        }

        if (LOG.isDebugEnabled() && onFailure != null) {
            onFailure.run();
        }

        status.expectSuccess(errorMsg);
    }
}
