package tech.ydb.importer.target;

import tech.ydb.core.Status;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.settings.BulkUpsertSettings;
import tech.ydb.table.values.ListValue;
import java.util.concurrent.CompletableFuture;

/**
 *
 * @author zinal
 */
public class YdbUpsertOp {

    private final SessionRetryContext retryCtx;
    private BulkUpsertSettings upsertSettings = null;
    private CompletableFuture<Status> status = null;
    private int currentRows = -1;
    private AnyCounter counter = null;

    public YdbUpsertOp(SessionRetryContext retryCtx) {
        this.retryCtx = retryCtx;
    }

    public BulkUpsertSettings getUpsertSettings() {
        if (upsertSettings == null)
            upsertSettings = new BulkUpsertSettings();
        return upsertSettings;
    }

    public int finish() {
        if (status == null)
            return 0;
        final int retval = (currentRows > 0) ? currentRows : 0;
        final String m = (counter==null) ? "bulk upsert problem" : counter.getIssueMessage();
        status.join().expectSuccess(m);
        if (counter != null)
            counter.addValue(retval);
        currentRows = -1;
        return retval;
    }

    public int start(String tablePath, ListValue newValue, AnyCounter counter) {
        if (newValue==null || newValue.isEmpty())
            return 0;
        final int retval = finish();
        this.currentRows = newValue.size();
        this.counter = counter;
        this.status = retryCtx.supplyStatus(
                session -> session.executeBulkUpsert(tablePath, newValue, getUpsertSettings())
        );
        return retval;
    }

}
