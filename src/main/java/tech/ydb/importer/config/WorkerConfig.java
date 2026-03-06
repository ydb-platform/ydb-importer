package tech.ydb.importer.config;

import org.jdom2.Element;

/**
 *
 * @author zinal
 */
public class WorkerConfig extends tech.ydb.importer.config.JdomHelper {

    private int readerPoolSize = 1;
    private int writerPoolSize = 0;
    private int bufferCount = 0;
    private boolean useArrow = false;

    public WorkerConfig() {
    }

    public WorkerConfig(Element c) {
        this.readerPoolSize = getInt(getSingleChild(c, "reader-pool"), "size");
        if (this.readerPoolSize < 1) {
            this.readerPoolSize = 1;
        }
        if (this.readerPoolSize > 1000) {
            this.readerPoolSize = 1000;
        }
        Element writerEl = getOneChild(c, "writer-pool");
        if (writerEl != null) {
            this.writerPoolSize = getInt(writerEl, "size");
        }
    }

    public int getReaderPoolSize() {
        return readerPoolSize;
    }

    public void setReaderPoolSize(int readerPoolSize) {
        this.readerPoolSize = readerPoolSize;
    }

    public int getWriterPoolSize() {
        return writerPoolSize > 0 ? writerPoolSize : readerPoolSize;
    }

    public void setWriterPoolSize(int writerPoolSize) {
        this.writerPoolSize = writerPoolSize;
    }

    public int getBufferCount() {
        return bufferCount > 0 ? bufferCount : readerPoolSize;
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
