package tech.ydb.importer.config;

import org.jdom2.Element;

/**
 *
 * @author zinal
 */
public class WorkerConfig extends tech.ydb.importer.config.JdomHelper {

    private int poolSize = 1;

    public WorkerConfig() {
    }

    public WorkerConfig(Element c) {
        this.poolSize = getInt(getSingleChild(c, "pool"), "size");
        if (this.poolSize < 1) {
            this.poolSize = 1;
        }
        if (this.poolSize > 1000) {
            this.poolSize = 1000;
        }
    }

    public int getPoolSize() {
        return poolSize;
    }

    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }

}
