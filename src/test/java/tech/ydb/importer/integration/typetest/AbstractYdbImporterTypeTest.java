package tech.ydb.importer.integration.typetest;

import tech.ydb.importer.integration.common.AbstractYdbImporterIntegrationTest;

/**
 * Base class for type-mapping round-trip tests.
 */
public abstract class AbstractYdbImporterTypeTest extends AbstractYdbImporterIntegrationTest {

    protected String createTableSuffix() {
        return "";
    }

    protected TypeTestBuilder typeTest() {
        return new TypeTestBuilder(this);
    }
}
