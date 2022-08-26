package ydb.importer.config;

import org.jdom2.Element;
import static ydb.importer.config.JdomHelper.*;

/**
 *
 * @author zinal
 */
public class WorkerConfig {
    
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
