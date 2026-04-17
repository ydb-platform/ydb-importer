package tech.ydb.importer.integration.verification;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import tech.ydb.importer.integration.common.YdbSchemaReader;
import tech.ydb.importer.integration.common.YdbSchemaReader.YdbTableInfo;

/** Verifies that rows imported into YDB match the scenario oracle. */
public final class ScenarioVerifier {

    private ScenarioVerifier() {
    }

    /** Drops scenarios whose required features are not in the supported set. */
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

    /**
     * Streams rows from {@code ydbPath} and compares each with the oracle.
     * If the scenario requires BLOBs, fetches and compares them afterwards.
     */
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
                if (errors.size() >= 20) {
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
}
