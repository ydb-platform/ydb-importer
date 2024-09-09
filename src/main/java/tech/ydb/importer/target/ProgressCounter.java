package tech.ydb.importer.target;

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Async data load progress measurement and tracing.
 *
 * @author zinal
 */
public class ProgressCounter implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ProgressCounter.class);

    private final AtomicLong readedRows;
    private final AtomicLong writedRows;
    private final AtomicLong blobRows;
    private final Thread workerThread;
    private final long startedAt;

    public ProgressCounter() {
        this.readedRows = new AtomicLong(0L);
        this.writedRows = new AtomicLong(0L);
        this.blobRows = new AtomicLong(0L);

        this.workerThread = new Thread(new ProgressWorker());
        this.workerThread.setDaemon(true);
        this.workerThread.setName("AsyncProgressThread");

        this.startedAt = System.currentTimeMillis();
    }

    public void start() {
        this.workerThread.start();
    }

    @Override
    public void close() {
        try {
            this.workerThread.interrupt();
            this.workerThread.join();
        } catch (InterruptedException ex) {
            LOG.warn("Process worker stopping was interrupted", ex);
            Thread.currentThread().interrupt();
        }
    }

    public long addReadedRows(int count) {
        return readedRows.addAndGet(count);
    }

    public long addWritedRows(int count) {
        return writedRows.addAndGet(count);
    }

    public long addBlobRows(int count) {
        return blobRows.addAndGet(count);
    }

    private class ProgressWorker implements Runnable {
        private long lastReaded = 0;
        private long lastWrited = 0;
        private long lastBlobs = 0;
        private long lastTs = 0;

        @Override
        @SuppressWarnings("SleepWhileInLoop")
        public void run() {
            lastReaded = readedRows.get();
            lastWrited = writedRows.get();
            lastBlobs = blobRows.get();
            lastTs = System.currentTimeMillis();

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(500L);
                    traceIfNeeded();
                } catch (InterruptedException ix) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            traceFinal();
        }

        private void traceIfNeeded() {
            long ts = System.currentTimeMillis();
            final long diff = ts - lastTs;
            if (diff < 30000L) {
                return;
            }

            long readed = readedRows.get();
            long writed = writedRows.get();
            long blobs = blobRows.get();

            double readedRate = 1000d * (readed - lastReaded) / diff;
            double writedRate = 1000d * (writed - lastWrited) / diff;

            LOG.info("Progress: {} rows readed total [{} rows/sec], {} rows writed total [{} rows/sec]",
                    readed, String.format("%.2f", readedRate), writed, String.format("%.2f", writedRate));

            if (blobs > lastBlobs) {
                double blobsRate = 1000d * (blobs - lastBlobs) / diff;
                LOG.info("\t BLOB fragments: {} rows total [{} rows/sec]", blobs, String.format("%.2f", blobsRate));
            }

            lastTs = ts;
            lastReaded = readed;
            lastWrited = writed;
            lastBlobs = blobs;
        }

        private void traceFinal() {
            long ts = System.currentTimeMillis();
            final long diff = ts - startedAt;

            long readed = readedRows.get();
            long writed = writedRows.get();
            double readedRate = 1000d * (readed - lastReaded) / diff;
            double writedRate = 1000d * (writed - lastWrited) / diff;

            LOG.info("Final: {} rows readed total [{} rows/sec], {} rows writed total [{} rows/sec]",
                    readed, String.format("%.2f", readedRate), writed, String.format("%.2f", writedRate));
        }
    }
}
