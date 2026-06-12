package tech.ydb.importer.integration.sources.mariadb.columnstore;

import tech.ydb.importer.integration.verification.DialectLoader;
import tech.ydb.importer.integration.verification.LogicalType;
import tech.ydb.importer.integration.verification.TableScenario;

public final class MariaDbColumnStoreLoader extends DialectLoader {

    public static final MariaDbColumnStoreLoader INSTANCE =
            new MariaDbColumnStoreLoader();

    private MariaDbColumnStoreLoader() {
    }

    @Override
    protected String toDdl(LogicalType type) {
        switch (type) {
            case INT32:           return "INT";
            case INT64:           return "BIGINT";
            case DECIMAL_18_4:    return "DECIMAL(18, 4)";
            case STRING:          return "VARCHAR(255)";
            case BOOL:            return "BOOLEAN";
            case DATE:            return "DATE";
            case DATETIME:        return "DATETIME";
            default:
                throw new IllegalArgumentException("Unsupported: " + type);
        }
    }

    @Override
    protected void appendEngine(StringBuilder ddl, TableScenario scenario) {
        ddl.append(" ENGINE=Columnstore");
    }

    @Override
    public Object adjustExpected(LogicalType type, Object expected) {
        if (type == LogicalType.BOOL && expected instanceof Boolean) {
            return ((Boolean) expected) ? 1 : 0;
        }
        return expected;
    }
}
