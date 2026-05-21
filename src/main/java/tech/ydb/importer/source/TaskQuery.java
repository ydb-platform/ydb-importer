package tech.ydb.importer.source;

/**
 * A single SQL query of a reading task.
 */
public class TaskQuery {

    private final String name;
    private final String sql;

    public TaskQuery(String name, String sql) {
        this.name = name;
        this.sql = sql;
    }

    public String getName() {
        return name;
    }

    public String getSql() {
        return sql;
    }

}
