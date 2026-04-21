package tech.ydb.importer.integration.verification;

import java.util.Map;

/** Produces expected row values for a scenario */
public interface RowOracle {

    long rowCount();

    Map<String, Object> expectedFor(long id);

    default byte[] expectedBlobFor(long id) {
        return null;
    }
}
