package tech.ydb.importer.config;

import org.jdom2.Element;

/**
 *
 * @author zinal
 */
public class WorkerConfig extends tech.ydb.importer.config.JdomHelper {

    private static final int MAX_SIZE = 1000;

    private int readerPoolSize = 1;
    private int writerPoolSize = 1;
    private int bufferCount = 1;
    private boolean useArrow = false;

    public WorkerConfig() {
    }

    public WorkerConfig(Element c) {
        this.readerPoolSize = validatedSize(getInt(getSingleChild(c, "reader-pool"), "size"));

        Element writerEl = getOneChild(c, "writer-pool");
        this.writerPoolSize = (writerEl != null) ? validatedSize(getInt(writerEl, "size")) : this.readerPoolSize;

        Element bufEl = getOneChild(c, "buffer-count");
        this.bufferCount = (bufEl != null) ? validatedSize(getInt(bufEl)) : this.readerPoolSize;

        Element useArrowEl = getOneChild(c, "use-arrow");
        if (useArrowEl != null) {
            this.useArrow = parseBoolean(useArrowEl, null, getText(useArrowEl));
        }
    }

    private static int validatedSize(int v) {
        if (v < 1) {
            return 1;
        }
        if (v > MAX_SIZE) {
            return MAX_SIZE;
        }
        return v;
    }

    public int getReaderPoolSize() {
        return readerPoolSize;
    }

    public void setReaderPoolSize(int readerPoolSize) {
        this.readerPoolSize = readerPoolSize;
    }

    public int getWriterPoolSize() {
        return writerPoolSize;
    }

    public void setWriterPoolSize(int writerPoolSize) {
        this.writerPoolSize = writerPoolSize;
    }

    public int getBufferCount() {
        return bufferCount;
    }

    public void setBufferCount(int bufferCount) {
        this.bufferCount = bufferCount;
    }

    public boolean isUseArrow() {
        return useArrow;
    }

    public void setUseArrow(boolean useArrow) {
        this.useArrow = useArrow;
    }

}
