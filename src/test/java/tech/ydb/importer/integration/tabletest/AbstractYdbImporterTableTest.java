package tech.ydb.importer.integration.tabletest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import tech.ydb.importer.integration.common.AbstractYdbImporterIntegrationTest;
import tech.ydb.importer.integration.common.YdbImporterRunner;
import tech.ydb.importer.integration.common.YdbSchemaReader;

public abstract class AbstractYdbImporterTableTest extends AbstractYdbImporterIntegrationTest {

    protected TableTestBuilder tableTest(String schema, String table) {
        return new TableTestBuilder(this, schema, table);
    }

    protected ImportGroup importTogether() {
        return new ImportGroup();
    }

    protected static byte[] filled(int size, byte value) {
        byte[] arr = new byte[size];
        Arrays.fill(arr, value);
        return arr;
    }

    protected static String hex(byte[] data) {
        StringBuilder sb = new StringBuilder(data.length * 2);
        for (byte b : data) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    /** SQL expression that produces a BLOB from the given bytes. Override per source DB. */
    protected String smallBlob(byte[] data) {
        throw new UnsupportedOperationException("override smallBlob() for the source DB");
    }

    /** SQL expression for a BLOB of {@code count} copies of {@code hexPair}. Override per source DB. */
    protected String bigBlob(String hexPair, int count) {
        throw new UnsupportedOperationException("override bigBlob() for the source DB");
    }

    protected final class ImportGroup {
        private final List<TableTestBuilder> tables = new ArrayList<>();
        private boolean useArrow;

        public ImportGroup withArrow(boolean value) {
            this.useArrow = value;
            return this;
        }

        public ImportGroup add(TableTestBuilder table) {
            this.tables.add(table);
            return this;
        }

        public void run() throws Exception {
            if (tables.isEmpty()) {
                throw new IllegalStateException("importTogether requires at least one table");
            }
            TableTestBuilder primary = tables.get(0);
            YdbImporterRunner.Builder builder = YdbImporterRunner.builder()
                    .source(sourceContainer(), sourceType())
                    .ydb(ydbContainer())
                    .table(primary.schema, primary.table)
                    .useArrow(useArrow);
            for (int i = 1; i < tables.size(); i++) {
                builder.addTable(tables.get(i).schema, tables.get(i).table);
            }

            List<String> targetPaths = new ArrayList<>();
            for (TableTestBuilder t : tables) {
                targetPaths.add(YdbImporterRunner.DEFAULT_TARGET_PREFIX
                        + "." + t.schema + "." + t.table);
            }

            try (YdbSchemaReader ydb = new YdbSchemaReader(
                    ydbContainer().getConnectionString())) {
                try {
                    for (TableTestBuilder t : tables) {
                        t.buildDdlIfNeeded();
                        t.executeSetup();
                    }
                    builder.run();
                    for (int i = 0; i < tables.size(); i++) {
                        tables.get(i).verifyPhase2(ydb, targetPaths.get(i));
                    }
                } finally {
                    for (TableTestBuilder t : tables) {
                        t.executeCleanup();
                    }
                    for (int i = 0; i < tables.size(); i++) {
                        tables.get(i).dropBlobTables(
                                ydb, targetPaths.get(i));
                        ydb.dropTable(targetPaths.get(i));
                    }
                }
            }
        }
    }
}
