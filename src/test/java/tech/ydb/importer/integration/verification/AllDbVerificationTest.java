package tech.ydb.importer.integration.verification;

import java.sql.Connection;
import java.sql.DriverManager;
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
import tech.ydb.importer.integration.common.YdbSchemaReader;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** Cross-DB integration test: runs all shop scenarios for every source DB. */
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
        return null;
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
                "Skipped via -Dverify.skip");
        if (!only.isEmpty()) {
            Assumptions.assumeTrue(only.contains(db.name),
                    "Not in -Dverify.only");
        }

        db.container.start();
        try {
            List<TableScenario> scenarios = ScenarioVerifier
                    .filterSupported(ShopScenarios.all(ROW_COUNT),
                            db.supported);

            try (Connection src = DriverManager.getConnection(
                    db.container.getJdbcUrl(),
                    db.container.getUsername(),
                    db.container.getPassword())) {
                if (db.loader.requiresSetSchema()) {
                    try (java.sql.Statement st = src.createStatement()) {
                        st.execute("SET SCHEMA " + db.schema);
                    }
                }
                for (TableScenario s : scenarios) {
                    db.loader.createTable(src, s);
                    db.loader.loadRows(src, s, 10_000, n -> { });
                }
            }

            YdbImporterRunner.Builder runner = YdbImporterRunner.builder()
                    .source(db.container, db.sourceType)
                    .ydb(ydbContainer())
                    .table(db.schema,
                            db.tableName(scenarios.get(0)));
            for (int i = 1; i < scenarios.size(); i++) {
                runner.addTable(db.schema,
                        db.tableName(scenarios.get(i)));
            }
            runner.run();

            try (YdbSchemaReader ydb = new YdbSchemaReader(
                    ydbContainer().getConnectionString())) {
                for (TableScenario s : scenarios) {
                    String path = YdbImporterRunner.DEFAULT_TARGET_PREFIX
                            + "." + db.schema + "."
                            + db.tableName(s);
                    VerificationResult r = ScenarioVerifier.verify(
                            ydb, s, path, db.loader);
                    assertTrue(r.ok(),
                            s.tableName() + ": " + r.errorsSummary());
                }
            }
        } finally {
            db.container.stop();
        }
    }
}
