package tech.ydb.importer.source;

import java.util.Collections;
import java.util.List;

/**
 * A named partition containing one or more chunks to read sequentially.
 */
public class PartitionInfo {

    private final String name;
    private final List<ChunkInfo> chunks;
    private int index;

    public PartitionInfo(String name, List<ChunkInfo> chunks) {
        this.name = name;
        this.chunks = chunks;
    }

    public PartitionInfo(String name, String querySql) {
        this(name, Collections.singletonList(new ChunkInfo(name, querySql)));
    }

    public String getName() {
        return name;
    }

    public List<ChunkInfo> getChunks() {
        return chunks;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

}
