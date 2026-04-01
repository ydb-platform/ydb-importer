package tech.ydb.importer.source;

/**
 * An atomic SQL query representing one offset range within a partition.
 */
public class ChunkInfo {

    private final String name;
    private final String querySql;

    public ChunkInfo(String name, String querySql) {
        this.name = name;
        this.querySql = querySql;
    }

    public String getName() {
        return name;
    }

    public String getQuerySql() {
        return querySql;
    }

}
