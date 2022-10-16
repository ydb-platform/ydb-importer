package tech.ydb.importer.target;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Async data load progress measurement and tracing.
 * @author zinal
 */
public class ProgressCounter implements AutoCloseable {

    private final static org.slf4j.Logger LOG = org.slf4j.LoggerFactory
            .getLogger(ProgressCounter.class);

    private final ProgressWorker worker;
    private final AtomicLong normalRows;
    private final AtomicLong blobRows;
    private final long tvStart;
    private long tvCur;
    private long normalPrev;
    private long blobPrev;

    public ProgressCounter() {
        this.worker = new ProgressWorker(this);
        this.normalRows = new AtomicLong(0L);
        this.blobRows = new AtomicLong(0L);
        this.normalPrev = 0L;
        this.blobPrev = 0L;
        this.tvStart = System.currentTimeMillis();
        this.tvCur = this.tvStart;
        Thread t = new Thread(worker);
        t.setDaemon(true);
        t.setName("AsyncProgressThread");
        t.start();
    }

    @Override
    public void close() {
        worker.stop();
    }

    public long addNormal(int count) {
        return normalRows.addAndGet(count);
    }

    public long addBlob(int count) {
        return blobRows.addAndGet(count);
    }

    public long getNormal() {
        return normalRows.get();
    }

    public long getBlob() {
        return normalRows.get();
    }

    private void traceIfNeeded() {
        final long cur = System.currentTimeMillis();
        final long diff = cur - tvCur;
        if (diff < 30000L)
            return;
        final long tmp_normal = normalRows.get();
        final long inc_normal = tmp_normal - normalPrev;
        final double rate_normal = ((double)inc_normal) * 1000.0 / ((double)diff);

        final long tmp_blob = blobRows.get();
        final long inc_blob = tmp_blob - blobPrev;
        final double rate_blob = ((double)inc_blob) * 1000.0 / ((double)diff);

        LOG.info("Progress: {} rows total, {} rows increment ({} rows/sec)",
            tmp_normal, inc_normal, String.format("%.2f", rate_normal));
        if (inc_blob > 0L) {
            LOG.info("\t BLOB fragments: {} rows total, {} rows increment ({} rows/sec)",
                tmp_blob, inc_blob, String.format("%.2f", rate_blob));
        }

        tvCur = cur;
        normalPrev = tmp_normal;
        blobPrev = tmp_blob;
    }

    private void traceFinal() {
        final long cur = System.currentTimeMillis();
        final long diff = cur - tvStart;
        final long tmp_sumops = normalRows.get();
        final double rate = ((double)tmp_sumops) * 1000.0 / ((double)diff);

        LOG.info("Final: {} rows total ({} rows/sec average)",
            tmp_sumops, String.format("%.2f", rate));
    }

    public static final class ProgressWorker implements Runnable {

        private final ProgressCounter owner;
        private volatile boolean active;

        private ProgressWorker(ProgressCounter owner) {
            this.owner = owner;
            this.active = true;
        }

        private void stop() {
            active = false;
        }

        @Override
        public void run() {
            while (active) {
                try { Thread.sleep(500L); } catch(InterruptedException ix) {}
                if (active)
                    owner.traceIfNeeded();
            }
            owner.traceFinal();
        }
        
    }

}
