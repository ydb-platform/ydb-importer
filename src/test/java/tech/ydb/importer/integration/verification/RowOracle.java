package tech.ydb.importer.integration.verification;

import java.util.Map;

public interface RowOracle {

    long rowCount();

    Map<String, Object> expectedFor(long id);

    default byte[] expectedBlobFor(long id) {
        return null;
    }
}
