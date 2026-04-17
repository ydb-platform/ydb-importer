package tech.ydb.importer.integration.verification;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

import tech.ydb.importer.integration.common.YdbRowMatcher;

public final class StreamingVerifier {

    private static final int MAX_ERRORS = 20;

    private final RowOracle oracle;
    private final List<ColumnSpec> columns;
    private final String keyColumn;
    private final DialectLoader loader;
    private final BitSet seen;
    private final List<String> errors = new ArrayList<>();
    private long matched = 0;

    public StreamingVerifier(TableScenario scenario, DialectLoader loader) {
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

    public void onRow(Map<String, Object> actual) {
        if (errors.size() >= MAX_ERRORS) {
            return;
        }

        Object rawId = actual.get(keyColumn);
        if (rawId == null) {
            recordError("Row without '" + keyColumn + "' column");
            return;
        }
        long id = ((Number) rawId).longValue();
        if (id < 1 || id > oracle.rowCount()) {
            recordError("id out of range: " + id);
            return;
        }
        int idx = (int) (id - 1);
        if (seen.get(idx)) {
            recordError("Duplicate id: " + id);
            return;
        }
        seen.set(idx);

        Map<String, Object> expected = oracle.expectedFor(id);
        for (ColumnSpec col : columns) {
            Object exp = loader.adjustExpected(col.type(),
                    expected.get(col.name()));
            Object act = actual.get(col.name());
            if (!YdbRowMatcher.valuesEqual(exp, act)) {
                recordError("id=" + id + " col=" + col.name()
                        + " expected=" + YdbRowMatcher.describe(exp)
                        + " actual=" + YdbRowMatcher.describe(act));
                return;
            }
        }
        matched++;
    }

    public VerificationResult finish() {
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

    private void recordError(String msg) {
        errors.add(msg);
    }
}
