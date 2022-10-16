package tech.ydb.importer.target;

import java.util.concurrent.atomic.AtomicLong;

/**
 * BLOB value handling counter implementation.
 * @author zinal
 */
public class BlobCounter implements AnyCounter {

    private final ProgressCounter owner;
    private final String issueMessage;
    private final AtomicLong value = new AtomicLong(0L);

    public BlobCounter(String issueMessage) {
        this.issueMessage = issueMessage;
        this.owner = null;
    }

    public BlobCounter(String issueMessage, ProgressCounter owner) {
        this.issueMessage = issueMessage;
        this.owner = owner;
    }

    public String getIssueMessage() {
        return issueMessage;
    }

    @Override
    public long addValue(int v) {
        v = (v > 0) ? v : 0;
        if (owner != null)
            owner.addBlob(v);
        return value.addAndGet(v);
    }

    @Override
    public long getValue() {
        return value.get();
    }

}
