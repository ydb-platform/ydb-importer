package ydb.importer.target;

/**
 *
 * @author zinal
 */
public interface AnyCounter {

    long addValue(int v);

    long getValue();
    
    String getIssueMessage();

}
