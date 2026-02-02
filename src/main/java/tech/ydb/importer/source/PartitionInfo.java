package tech.ydb.importer.source;

/**
 * A named SQL chunk representing a partition or offset range to read.
 */
public class PartitionInfo {

    private final String name;
    private final String querySql;

    public PartitionInfo(String name, String querySql) {
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
