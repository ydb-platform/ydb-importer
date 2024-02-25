package tech.ydb.importer.target;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Counter implementation for row upsert iperations.
 *
 * @author zinal
 */
public class RowCounter implements AnyCounter {

    private final ProgressCounter owner;
    private final String issueMessage;
    private final AtomicLong value = new AtomicLong(0L);

    public RowCounter(String issueMessage) {
        this.issueMessage = issueMessage;
        this.owner = null;
    }

    public RowCounter(String issueMessage, ProgressCounter owner) {
        this.issueMessage = issueMessage;
        this.owner = owner;
    }

    public String getIssueMessage() {
        return issueMessage;
    }

    @Override
    public long addValue(int v) {
        v = (v > 0) ? v : 0;
        if (owner != null) {
            owner.addNormal(v);
        }
        return value.addAndGet(v);
    }

    @Override
    public long getValue() {
        return value.get();
    }

}
