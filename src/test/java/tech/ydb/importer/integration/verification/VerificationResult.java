package tech.ydb.importer.integration.verification;

import java.util.Collections;
import java.util.List;

public final class VerificationResult {

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
