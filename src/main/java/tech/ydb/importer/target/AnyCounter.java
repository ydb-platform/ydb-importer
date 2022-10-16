package tech.ydb.importer.target;

/**
 * Interface for a simple statistical counter.
 * @author zinal
 */
public interface AnyCounter {

    long addValue(int v);

    long getValue();
    
    String getIssueMessage();

}
