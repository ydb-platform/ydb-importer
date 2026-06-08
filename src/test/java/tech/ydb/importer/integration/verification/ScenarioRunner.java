package tech.ydb.importer.integration.verification;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.LongConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.importer.YdbImporter;
import tech.ydb.importer.integration.common.LocalYdbTestContainer;
import tech.ydb.importer.integration.common.YdbImporterRunner;
import tech.ydb.importer.integration.common.YdbSchemaReader;
import tech.ydb.importer.integration.verification.ScenarioVerifier.VerificationResult;

/**
 * Loads scenarios, runs the importer, verifies results
 * Shared by the integration test and the benchmark
 */
public final class ScenarioRunner implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ScenarioRunner.class);

    public final SourceDbProfile profile;
    public final List<TableScenario> scenarios;
    private final long totalRows;

    public ScenarioRunner(SourceDbProfile profile, List<TableScenario> scenarios) {
        if (scenarios.isEmpty()) {
            throw new IllegalArgumentException("ScenarioRunner requires at least one scenario");
        }
        this.profile = profile;
        this.scenarios = scenarios;
        long total = 0;
        for (TableScenario s : scenarios) {
            total += s.oracle().rowCount();
        }
        this.totalRows = total;
    }

    public long totalRows() {
        return totalRows;
    }

    public void startSource() {
        profile.container.start();
    }

    /** Creates tables and loads all scenario rows */
    public void loadSource(int batchSize, int poolSize, LongConsumer onBatch)
            throws Exception {
        int resolvedBatch = batchSize > 0
                ? batchSize : profile.loader.rowsPerInsertStatement();
        int resolvedPool = resolvePool(poolSize);

        if (resolvedPool == 1) {
            try (Connection c = profile.openConnection()) {
                for (TableScenario s : scenarios) {
                    profile.loader.createTable(c, s);
                    profile.loader.loadRows(c, s, resolvedBatch, onBatch);
                }
            }
            return;
        }

        ExecutorService pool = Executors.newFixedThreadPool(resolvedPool);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (TableScenario s : scenarios) {
                futures.add(pool.submit(() -> {
                    try (Connection c = profile.openConnection()) {
                        profile.loader.createTable(c, s);
                        profile.loader.loadRows(c, s, resolvedBatch, onBatch);
                    }
                    return null;
                }));
            }
            for (Future<?> f : futures) {
                f.get();
            }
        } finally {
            pool.shutdown();
        }
    }

    /** Runs the YDB importer over all scenarios */
    public void runImport(LocalYdbTestContainer ydb, int batchSize,
                          int poolSize, int fetchSize, String targetPrefix)
            throws Exception {
        runImport(ydb, batchSize, poolSize, fetchSize, true, targetPrefix);
    }

    public void runImport(LocalYdbTestContainer ydb, int batchSize,
                          int poolSize, int fetchSize, boolean usePartitions,
                          String targetPrefix)
            throws Exception {
        runImport(ydb, batchSize, poolSize, fetchSize, usePartitions, false, targetPrefix);
    }

    public void runImport(LocalYdbTestContainer ydb, int batchSize,
                          int poolSize, int fetchSize, boolean usePartitions,
                          boolean useArrow, String targetPrefix)
            throws Exception {
        YdbImporterRunner.Builder builder = YdbImporterRunner.builder()
                .source(profile.container, profile.sourceType)
                .ydb(ydb)
                .table(profile.schema, profile.tableName(scenarios.get(0)))
                .usePartitions(usePartitions)
                .useArrow(useArrow);
        if (batchSize > 0) {
            builder.maxBatchRows(batchSize);
        }
        if (poolSize > 0) {
            builder.poolSize(poolSize);
        }
        if (fetchSize > 0) {
            builder.fetchSize(fetchSize);
        }
        if (targetPrefix != null) {
            builder.targetPrefix(targetPrefix);
        }
        for (int i = 1; i < scenarios.size(); i++) {
            builder.addTable(profile.schema, profile.tableName(scenarios.get(i)));
        }
        new YdbImporter(builder.buildConfig()).run();
    }

    public List<Failure> verifyAll(LocalYdbTestContainer ydb, String targetPrefix) {
        List<Failure> failures = new ArrayList<>();
        try (YdbSchemaReader reader = new YdbSchemaReader(ydb.getConnectionString())) {
            for (TableScenario s : scenarios) {
                String path = targetPrefix + "." + profile.schema
                        + "." + profile.tableName(s);
                VerificationResult r = ScenarioVerifier.verify(
                        reader, s, path, profile.loader);
                if (!r.ok()) {
                    failures.add(new Failure(s.tableName(), r.errorsSummary()));
                }
            }
        }
        return failures;
    }

    @Override
    public void close() {
        try {
            profile.container.stop();
        } catch (RuntimeException ex) {
            LOG.warn("Failed to stop source [{}]", profile.name, ex);
        }
    }

    private int resolvePool(int requested) {
        if (requested > 0) {
            return requested;
        }
        int profilePool = profile.loader.loaderPoolSize();
        return profilePool > 0 ? profilePool : 1;
    }

    public static final class Failure {
        public final String table;
        public final String summary;

        public Failure(String table, String summary) {
            this.table = table;
            this.summary = summary;
        }
    }
}
