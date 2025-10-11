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

    private final AtomicLong numRowsRRead;
    private final AtomicLong numRowsWritten;
    private final AtomicLong numRowsBlob;
    private final Thread workerThread;
    private final long startedAt;

    public ProgressCounter() {
        this.numRowsRRead = new AtomicLong(0L);
        this.numRowsWritten = new AtomicLong(0L);
        this.numRowsBlob = new AtomicLong(0L);

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

    public long countReadRows(int count) {
        return numRowsRRead.addAndGet(count);
    }

    public long countWrittenRows(int count) {
        return numRowsWritten.addAndGet(count);
    }

    public long countBlobRows(int count) {
        return numRowsBlob.addAndGet(count);
    }

    private class ProgressWorker implements Runnable {

        private long lastRead = 0;
        private long lastWritten = 0;
        private long lastBlobs = 0;
        private long lastTs = 0;

        @Override
        @SuppressWarnings("SleepWhileInLoop")
        public void run() {
            lastRead = numRowsRRead.get();
            lastWritten = numRowsWritten.get();
            lastBlobs = numRowsBlob.get();
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

            long readed = numRowsRRead.get();
            long writed = numRowsWritten.get();
            long blobs = numRowsBlob.get();

            double readedRate = 1000d * (readed - lastRead) / diff;
            double writedRate = 1000d * (writed - lastWritten) / diff;

            LOG.info("Progress: {} rows read total [{} rows/sec], {} rows written total [{} rows/sec]",
                    readed, String.format("%.2f", readedRate), writed, String.format("%.2f", writedRate));

            if (blobs > lastBlobs) {
                double blobsRate = 1000d * (blobs - lastBlobs) / diff;
                LOG.info("\t BLOB fragments: {} rows total [{} rows/sec]", blobs, String.format("%.2f", blobsRate));
            }

            lastTs = ts;
            lastRead = readed;
            lastWritten = writed;
            lastBlobs = blobs;
        }

        private void traceFinal() {
            long ts = System.currentTimeMillis();
            final long diff = ts - startedAt;

            long readed = numRowsRRead.get();
            long writed = numRowsWritten.get();
            double readedRate = 1000d * (readed - lastRead) / diff;
            double writedRate = 1000d * (writed - lastWritten) / diff;

            LOG.info("Final: {} rows read total [{} rows/sec], {} rows written total [{} rows/sec]",
                    readed, String.format("%.2f", readedRate), writed, String.format("%.2f", writedRate));
        }
    }
}
