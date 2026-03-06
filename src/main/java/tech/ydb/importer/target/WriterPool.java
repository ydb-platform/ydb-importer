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
 * Pool of writer threads that consume batches from a bounded queue
 * and upload them to YDB.
 */
public class WriterPool implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(WriterPool.class);
    private static final long SHUTDOWN_TIMEOUT_MS = 60_000;

    private final ExecutorService executor;
    private final int writerCount;
    private final int queueCapacity;
    private final AtomicReference<Exception> firstError = new AtomicReference<>();

    private final BlockingQueue<TaggedBatch> queue;

    public WriterPool(int writerCount, int queueCapacity) {
        this.writerCount = writerCount;
        this.queueCapacity = queueCapacity;

        final AtomicInteger threadId = new AtomicInteger();
        this.executor = Executors.newFixedThreadPool(writerCount, r -> {
            Thread t = new Thread(r, "ydb-writer-" + threadId.getAndIncrement());
            t.setDaemon(true);
            return t;
        });

        this.queue = new ArrayBlockingQueue<>(queueCapacity);
    }

    public void start() {
        LOG.info("Starting {} writer threads (queue capacity {})",
                writerCount, queueCapacity);
        for (int i = 0; i < writerCount; i++) {
            executor.submit(this::writerLoop);
        }
    }

    /**
     * Enqueues a batch for writing. Blocks when the queue is full.
     */
    public void submit(TaggedBatch batch) throws Exception {
        checkError();
        queue.put(batch);
        checkError();
    }

    private void checkError() throws Exception {
        Exception err = firstError.get();
        if (err != null) {
            throw new RuntimeException("Writer pool has a failed writer", err);
        }
    }

    /**
     * Signals writers to stop by sending a poison pill,
     * then waits for them to drain the queue and terminate.
     */
    public void shutdownAndWait() throws Exception {
        queue.put(TaggedBatch.POISON);
        executor.shutdown();
        if (!executor.awaitTermination(SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            LOG.error("Writer pool did not terminate within {} ms, forcing shutdown",
                    SHUTDOWN_TIMEOUT_MS);
            executor.shutdownNow();
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
                TaggedBatch batch = queue.take();
                if (batch == TaggedBatch.POISON) {
                    queue.offer(TaggedBatch.POISON);
                    return;
                }
                batch.execute();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOG.error("Writer thread {} failed", Thread.currentThread().getName(), e);
            firstError.compareAndSet(null, e);
            queue.clear();
        }
    }
}
