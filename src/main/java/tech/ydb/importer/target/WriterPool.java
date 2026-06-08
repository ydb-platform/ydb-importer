package tech.ydb.importer.target;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bounded queue of pending upserts processed by writer threads.
 */
public class WriterPool implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(WriterPool.class);
    private static final long SHUTDOWN_TIMEOUT_MS = 60_000;
    private static final long FORCE_SHUTDOWN_TIMEOUT_MS = 10_000;

    private final ExecutorService executor;
    private final BlockingQueue<UploadBatch> queue;
    private final int writerCount;
    private final ProgressCounter progress;
    private final AtomicReference<Exception> firstError = new AtomicReference<>();

    public WriterPool(int writerCount, int queueCapacity, ProgressCounter progress) {
        this.writerCount = writerCount;
        this.queue = new ArrayBlockingQueue<>(queueCapacity);
        this.progress = progress;

        final AtomicInteger threadId = new AtomicInteger();
        this.executor = Executors.newFixedThreadPool(writerCount, r -> {
            Thread t = new Thread(r, "ydb-writer-" + threadId.getAndIncrement());
            t.setDaemon(false);
            return t;
        });

        LOG.info("Starting {} writer threads (queue capacity {})", writerCount, queueCapacity);
        for (int i = 0; i < writerCount; i++) {
            executor.submit(this::writerLoop);
        }
    }

    public void submit(UploadBatch batch) throws Exception {
        checkError();
        queue.put(batch);
        checkError();
    }

    private void checkError() throws Exception {
        Exception err = firstError.get();
        if (err != null) {
            throw err;
        }
    }

    public void shutdownAndWait() throws Exception {
        for (int i = 0; i < writerCount; i++) {
            queue.put(UploadBatch.SHUTDOWN_SIGNAL);
        }
        executor.shutdown();
        if (!executor.awaitTermination(SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            LOG.error("Writer pool did not terminate within {} ms, forcing shutdown",
                    SHUTDOWN_TIMEOUT_MS);
            executor.shutdownNow();
            if (!executor.awaitTermination(FORCE_SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("Writer pool did not terminate after forced shutdown ("
                        + (SHUTDOWN_TIMEOUT_MS + FORCE_SHUTDOWN_TIMEOUT_MS) + " ms)");
            }
        }

        Exception err = firstError.get();
        if (err != null) {
            throw err;
        }
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }

    private void writerLoop() {
        try {
            while (true) {
                UploadBatch batch = queue.take();
                if (batch == UploadBatch.SHUTDOWN_SIGNAL) {
                    return;
                }
                long started = System.nanoTime();
                try {
                    batch.getOp().upload(batch.getData(), batch.getRowCount(), batch.getOnFailure());
                } finally {
                    progress.countUploadBatch(System.nanoTime() - started);
                }
            }
        } catch (InterruptedException e) {
            firstError.compareAndSet(null, e);
            queue.clear();
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOG.error("Writer thread {} failed", Thread.currentThread().getName(), e);
            firstError.compareAndSet(null, e);
            queue.clear();
        }
    }
}
