package tech.ydb.importer.integration.verification;

import java.util.List;
import java.util.TimeZone;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import tech.ydb.importer.integration.common.AbstractYdbImporterIntegrationTest;
import tech.ydb.importer.integration.common.YdbImporterRunner;
import tech.ydb.importer.integration.verification.ScenarioRunner.Failure;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** Cross-DB integration test: runs all shop scenarios for every source DB */
public class AllDbVerificationTest
        extends AbstractYdbImporterIntegrationTest {

    private static final long ROW_COUNT = 10_000;
    private static final String DB_NAME = "import_test";

    private static TimeZone originalTz;

    @BeforeAll
    static void init() {
        originalTz = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    }

    @AfterAll
    static void restore() {
        if (originalTz != null) {
            TimeZone.setDefault(originalTz);
        }
    }

    @Override
    public SourceDb sourceDb() {
        throw new UnsupportedOperationException(
                "AllDbVerificationTest manages sources via ScenarioRunner");
    }

    static Stream<SourceDbProfile> databases() {
        return SourceDbProfile.all(DB_NAME).stream();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("databases")
    void shopScenariosRoundTrip(SourceDbProfile db) throws Exception {
        String skip = System.getProperty("verify.skip", "");
        String only = System.getProperty("verify.only", "");
        Assumptions.assumeFalse(skip.contains(db.name),
                "Skipped by -Dverify.skip");
        if (!only.isEmpty()) {
            Assumptions.assumeTrue(only.contains(db.name),
                    "Not in -Dverify.only");
        }

        List<TableScenario> scenarios = ScenarioVerifier.filterSupported(
                ShopScenarios.all(ROW_COUNT), db.supported);

        try (ScenarioRunner runner = new ScenarioRunner(db, scenarios)) {
            runner.startSource();
            runner.loadSource(10_000, 1, n -> { });
            runner.runImport(ydbContainer(), -1, -1, -1, null);

            List<Failure> failures = runner.verifyAll(ydbContainer(),
                    YdbImporterRunner.DEFAULT_TARGET_PREFIX);
            assertTrue(failures.isEmpty(), () -> formatFailures(failures));
        }
    }

    private static String formatFailures(List<Failure> failures) {
        StringBuilder sb = new StringBuilder();
        for (Failure f : failures) {
            sb.append(f.table).append(": ").append(f.summary).append('\n');
        }
        return sb.toString();
    }
}
