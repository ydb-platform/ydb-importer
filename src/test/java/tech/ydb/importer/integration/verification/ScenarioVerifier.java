package tech.ydb.importer.integration.verification;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import tech.ydb.importer.integration.common.YdbRowMatcher;
import tech.ydb.importer.integration.common.YdbSchemaReader;
import tech.ydb.importer.integration.common.YdbSchemaReader.YdbTableInfo;
import tech.ydb.importer.integration.verification.TableScenario.Feature;

/** Verifies that rows imported into YDB match the scenario oracle */
public final class ScenarioVerifier {

    private static final int MAX_ERRORS = 20;

    private ScenarioVerifier() {
    }

    /** Drops scenarios whose required features are not in the supported set */
    public static List<TableScenario> filterSupported(
            List<TableScenario> all, Set<Feature> supported) {
        List<TableScenario> result = new ArrayList<>();
        for (TableScenario s : all) {
            if (s.requires(Feature.BLOB) && !supported.contains(Feature.BLOB)) {
                continue;
            }
            result.add(s);
        }
        return result;
    }

    /** Reads each YDB row, compares columns and BLOBs with the oracle */
    public static VerificationResult verify(YdbSchemaReader ydb,
                                            TableScenario scenario,
                                            String ydbPath,
                                            DialectLoader loader) {
        StreamingVerifier v = new StreamingVerifier(scenario, loader);
        ydb.streamRows(ydbPath, v::onRow);
        VerificationResult columns = v.finish();
        if (!columns.ok()) {
            return columns;
        }
        if (scenario.requires(Feature.BLOB)) {
            return verifyBlobs(ydb, scenario, ydbPath, columns);
        }
        return columns;
    }

    private static VerificationResult verifyBlobs(
            YdbSchemaReader ydb, TableScenario scenario,
            String ydbPath, VerificationResult base) {
        String blobCol = findColumn(ydb, ydbPath, scenario.blobColumn());
        String idCol = findColumn(ydb, ydbPath, scenario.keyColumn());
        List<String> errors = new ArrayList<>();
        for (long id = 1; id <= scenario.oracle().rowCount(); id++) {
            byte[] expected = scenario.oracle().expectedBlobFor(id);
            byte[] actual = ydb.readBlobBytes(
                    ydbPath, blobCol, idCol, id);
            if (!Arrays.equals(expected, actual)) {
                errors.add("blob mismatch id=" + id + " col=" + blobCol);
                if (errors.size() >= MAX_ERRORS) {
                    break;
                }
            }
        }
        if (errors.isEmpty()) {
            return base;
        }
        return new VerificationResult(base.matched(), base.expected(),
                errors);
    }

    private static String findColumn(YdbSchemaReader ydb, String path,
                                     String name) {
        YdbTableInfo info = ydb.describe(path);
        if (info.hasColumn(name)) {
            return name;
        }
        String upper = name.toUpperCase();
        if (info.hasColumn(upper)) {
            return upper;
        }
        return name;
    }

    public static final class VerificationResult {

        private final long matched;
        private final long expected;
        private final List<String> errors;

        public VerificationResult(long matched, long expected,
                                  List<String> errors) {
            this.matched = matched;
            this.expected = expected;
            this.errors = Collections.unmodifiableList(errors);
        }

        public boolean ok() {
            return errors.isEmpty() && matched == expected;
        }

        public long matched() {
            return matched;
        }

        public long expected() {
            return expected;
        }

        public String errorsSummary() {
            if (errors.isEmpty() && matched == expected) {
                return "OK: " + matched + " rows verified";
            }
            StringBuilder sb = new StringBuilder();
            if (matched != expected) {
                sb.append("Row count mismatch: matched=").append(matched)
                        .append(" expected=").append(expected).append('\n');
            }
            for (String err : errors) {
                sb.append(err).append('\n');
            }
            return sb.toString();
        }
    }

    private static final class StreamingVerifier {

        private final RowOracle oracle;
        private final List<ColumnSpec> columns;
        private final String keyColumn;
        private final DialectLoader loader;
        private final BitSet seen;
        private final List<String> errors = new ArrayList<>();
        private long matched = 0;

        StreamingVerifier(TableScenario scenario, DialectLoader loader) {
            this.oracle = scenario.oracle();
            this.columns = scenario.columns();
            this.keyColumn = scenario.keyColumn();
            this.loader = loader;
            long n = oracle.rowCount();
            if (n > Integer.MAX_VALUE) {
                throw new IllegalArgumentException(
                        "Row count exceeds BitSet capacity: " + n);
            }
            this.seen = new BitSet((int) n);
        }

        void onRow(Map<String, Object> actual) {
            if (errors.size() >= MAX_ERRORS) {
                return;
            }

            Object rawId = actual.get(keyColumn);
            if (rawId == null) {
                errors.add("Row without '" + keyColumn + "' column");
                return;
            }
            long id = ((Number) rawId).longValue();
            if (id < 1 || id > oracle.rowCount()) {
                errors.add("id out of range: " + id);
                return;
            }
            int idx = (int) (id - 1);
            if (seen.get(idx)) {
                errors.add("Duplicate id: " + id);
                return;
            }
            seen.set(idx);

            Map<String, Object> expected = oracle.expectedFor(id);
            for (ColumnSpec col : columns) {
                Object exp = loader.adjustExpected(col.type(),
                        expected.get(col.name()));
                Object act = actual.get(col.name());
                if (!YdbRowMatcher.valuesEqual(exp, act)) {
                    errors.add("id=" + id + " col=" + col.name()
                            + " expected=" + YdbRowMatcher.describe(exp)
                            + " actual=" + YdbRowMatcher.describe(act));
                    return;
                }
            }
            matched++;
        }

        VerificationResult finish() {
            if (matched < oracle.rowCount()) {
                reportMissing();
            }
            return new VerificationResult(matched, oracle.rowCount(),
                    new ArrayList<>(errors));
        }

        private void reportMissing() {
            int shown = 0;
            long n = oracle.rowCount();
            for (int idx = 0; idx < n && shown < 20
                    && errors.size() < MAX_ERRORS; idx++) {
                if (seen.get(idx)) {
                    continue;
                }
                long id = idx + 1L;
                Map<String, Object> row = oracle.expectedFor(id);
                StringBuilder sb = new StringBuilder();
                sb.append("missing id=").append(id).append(" {");
                boolean first = true;
                for (ColumnSpec col : columns) {
                    if (!first) {
                        sb.append(", ");
                    }
                    first = false;
                    sb.append(col.name()).append('=')
                            .append(YdbRowMatcher.describe(row.get(col.name())));
                }
                sb.append('}');
                errors.add(sb.toString());
                shown++;
            }
        }
    }
}
