package tech.ydb.importer.source;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Reading task for a source table with the SQL queries to execute.
 */
public class TaskInfo {

    private final String name;
    private final List<TaskQuery> queries;
    private int index;

    public TaskInfo(String name, List<TaskQuery> queries) {
        this.name = name;
        this.queries = Collections.unmodifiableList(new ArrayList<>(queries));
    }

    public TaskInfo(String name, String sql) {
        this(name, Collections.singletonList(new TaskQuery(name, sql)));
    }

    public String getName() {
        return name;
    }

    public List<TaskQuery> getQueries() {
        return queries;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

}
