package tech.ydb.importer.integration;

import java.util.Collections;
import java.util.List;

/**
 * Single dialect-specific end-to-end test case.
 */
public interface DialectCase {

    /**
     * Import case description.
     */
    ImportCase getImportCase();

    /**
     * SQL statements to prepare schema and data in the source database.
     */
    List<String> prepareSourceSql();

    /**
     * SQL statements to cleanup the source database after the test.
     */
    default List<String> cleanupSourceSql() {
        return Collections.emptyList();
    }
}
