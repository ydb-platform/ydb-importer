package tech.ydb.importer.integration;

import java.sql.Connection;

/**
 * Single dialect-specific end-to-end test case.
 */
public interface DialectCase {

    /**
     * Import case description.
     */
    ImportCase getImportCase();

    /**
     * Prepare schema and data in the source database.
     */
    void prepareSourceData(Connection connection) throws Exception;

    /**
     * Optional cleanup after test.
     */
    default void cleanupSourceData(Connection connection) throws Exception {
    }
}



