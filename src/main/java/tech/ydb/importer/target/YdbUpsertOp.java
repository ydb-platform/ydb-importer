package tech.ydb.importer.target;

import java.util.function.IntConsumer;

import tech.ydb.core.Status;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.settings.BulkUpsertSettings;
import tech.ydb.table.values.ListValue;

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

    public void upload(ListValue values) {
        if (values == null || values.isEmpty()) {
            return;
        }

        Status status = retryCtx.supplyStatus(
                session -> session.executeBulkUpsert(tablePath, values, upsertSettings)
        ).join();

        if (status.isSuccess()) {
            counter.accept(values.size());
            return;
        }

        if (LOG.isDebugEnabled()) {
            logValues(values);
        }

        status.expectSuccess(errorMsg);
    }

    private void logValues(ListValue values) {
        int size = values.size();
        LOG.debug("********************************");
        LOG.debug("Problematic data block dump START, size is {}", size);
        for (int i = 0; i < size; ++i) {
            LOG.debug("{} {}", i, values.get(i));
        }
        LOG.debug("Problematic data block dump FINISH");
        LOG.debug("********************************");
    }
}
