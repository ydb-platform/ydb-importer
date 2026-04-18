package tech.ydb.importer.benchmark;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.testcontainers.containers.JdbcDatabaseContainer;

import tech.ydb.importer.YdbImporter;
import tech.ydb.importer.config.SourceType;
import tech.ydb.importer.integration.common.LocalYdbTestContainer;
import tech.ydb.importer.integration.common.YdbImporterRunner;
import tech.ydb.importer.integration.common.YdbSchemaReader;
import tech.ydb.importer.integration.sources.mariadb.columnstore.ColumnStoreContainer;
import tech.ydb.importer.integration.sources.mariadb.columnstore.MariaDbColumnStoreLoader;
import tech.ydb.importer.integration.verification.DialectLoader;
import tech.ydb.importer.integration.verification.Feature;
import tech.ydb.importer.integration.verification.ScenarioVerifier;
import tech.ydb.importer.integration.verification.ShopScenarios;
import tech.ydb.importer.integration.verification.SourceDbProfile;
import tech.ydb.importer.integration.verification.TableScenario;
import tech.ydb.importer.integration.verification.VerificationResult;

/**
 * Benchmark entry point: loads shop scenarios into a source DB, imports them
 * into YDB under selected modes (ROW/ARROW, with/without partitions),
 * verifies row-by-row, and writes timings to bench-results/benchmark-N.json.
 */
public class VerificationBenchmark {

    private static final String TARGET_PREFIX = "bench";
    private static final String DEFAULT_DB_NAME = "bench_test";

    public static void main(String[] args) throws Exception {
        int rows = 10_000;
        int batchSize = 2000;
        int genBatchSize = -1;
        int poolSize = 4;
        int genPoolSize = 4;
        int fetchSize = 10_000;
        String sourcesArg = "postgres";
        String modesArg = "ROW+PART";
        boolean verify = true;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--rows":
                    rows = Integer.parseInt(args[++i]);
                    break;
                case "--batch-size":
                    batchSize = Integer.parseInt(args[++i]);
                    break;
                case "--gen-batch-size":
                    genBatchSize = Integer.parseInt(args[++i]);
                    break;
                case "--pool-size":
                    poolSize = Integer.parseInt(args[++i]);
                    break;
                case "--gen-pool-size":
                    genPoolSize = Integer.parseInt(args[++i]);
                    break;
                case "--fetch-size":
                    fetchSize = Integer.parseInt(args[++i]);
                    break;
                case "--sources":
                    sourcesArg = args[++i];
                    break;
                case "--no-verify":
                    verify = false;
                    break;
                case "--modes":
                    modesArg = args[++i];
                    break;
                case "--help":
                case "-h":
                    printHelp();
                    return;
                default:
                    System.err.println("Unknown argument: " + args[i]);
                    System.err.println("Use --help for usage.");
                    System.exit(1);
            }
        }

        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

        String[] allLabels = {"ROW", "ROW+PART", "ARROW", "ARROW+PART"};
        boolean[] allArrows = {false, false, true, true};
        boolean[] allParts = {false, true, false, true};
        List<Integer> selectedModes = parseModes(modesArg, allLabels);
        List<String> sources = parseSources(sourcesArg);

        System.out.printf("=== Verification Benchmark ===%n");
        System.out.printf("Sources:        %s%n", String.join(",", sources));
        System.out.printf("Modes:          %s%n", joinSelectedModes(selectedModes, allLabels));
        System.out.printf("Rows per table: %,d%n", rows);
        System.out.printf("Batch size:     %,d%n", batchSize);
        System.out.printf("Gen batch:      %s%n",
                genBatchSize > 0
                        ? String.format("%,d", genBatchSize)
                        : "per-dialect default");
        System.out.printf("Pool size:      %d%n", poolSize);
        System.out.printf("Gen pool:       %d (DB2 forced to 1)%n", genPoolSize);
        System.out.printf("Fetch size:     %,d%n%n", fetchSize);

        List<SourceRun> runs = new ArrayList<>();
        for (String name : sources) {
            runs.add(runSource(name, selectedModes, allLabels, allArrows,
                    allParts, rows, batchSize, genBatchSize, poolSize,
                    genPoolSize, fetchSize, verify));
        }

        printRowCounts(runs);
        printOverallSummary(runs);
        printFailures(runs);
        writeJson(rows, batchSize, poolSize, fetchSize, runs);

        if (anyError(runs)) {
            System.exit(1);
        }
    }

    private static SourceRun runSource(String name, List<Integer> selectedModes,
            String[] allLabels, boolean[] allArrows, boolean[] allParts,
            int rows, int batchSize, int genBatchSize, int poolSize,
            int genPoolSize, int fetchSize, boolean verify) {
        System.out.printf("=== Source: %s ===%n", name);
        SourceRun run = new SourceRun(name);
        SourceSetup setup = null;
        boolean startedOk = false;

        try {
            setup = createSource(name);
            if (!setup.alreadyStarted) {
                setup.container.start();
            }
            startedOk = true;

            List<TableScenario> scenarios = ScenarioVerifier.filterSupported(
                    ShopScenarios.all(rows), setup.supported);
            long totalRows = 0;
            for (TableScenario s : scenarios) {
                totalRows += s.oracle().rowCount();
            }
            run.totalRows = totalRows;
            System.out.printf("Tables: %d, total rows: %,d%n",
                    scenarios.size(), totalRows);

            System.out.printf("=== Loading source data [%s] ===%n", name);
            long genStart = System.currentTimeMillis();
            loadSourceData(setup, scenarios, genBatchSize, genPoolSize, totalRows);
            run.loadMs = System.currentTimeMillis() - genStart;
            System.out.printf("Loaded in %.1fs%n%n", run.loadMs / 1000.0);

            for (int idx : selectedModes) {
                LocalYdbTestContainer ydb = new LocalYdbTestContainer();
                ydb.start();
                try {
                    run.modes.add(runMode(allLabels[idx], allArrows[idx],
                            allParts[idx], scenarios, totalRows, setup, ydb,
                            batchSize, poolSize, fetchSize, verify));
                } finally {
                    ydb.stop();
                }
            }
        } catch (Exception e) {
            run.error = shortMessage(e);
            System.err.printf("Source [%s] failed: %s%n", name, run.error);
            e.printStackTrace();
        } finally {
            if (setup != null && startedOk) {
                try {
                    setup.stop();
                } catch (RuntimeException stopEx) {
                    System.err.printf("Failed to stop source [%s]: %s%n",
                            name, stopEx.getMessage());
                }
            }
        }
        return run;
    }

    private static String shortMessage(Throwable t) {
        String msg = t.getMessage();
        if (msg == null || msg.isEmpty()) {
            msg = t.getClass().getSimpleName();
        }
        int nl = msg.indexOf('\n');
        if (nl >= 0) {
            msg = msg.substring(0, nl);
        }
        return msg;
    }

    private static void printHelp() {
        System.out.println(String.join(System.lineSeparator(),
                "Usage: VerificationBenchmark [options]",
                "",
                "Options:",
                "  --rows N              rows per table (default 10000)",
                "  --batch-size N        YDB importer batch size (default 2000)",
                "  --gen-batch-size N    rows per source INSERT (default 50000)",
                "  --pool-size N         importer reader pool size (default 4)",
                "  --gen-pool-size N     threads for source data loading (default 4,",
                "                        DB2 forced to 1)",
                "  --fetch-size N        JDBC fetchSize (default 10000)",
                "  --sources LIST        comma-separated from: clickhouse, mariadb, mysql,",
                "                        oracle, db2, postgres, greenplum, vertica, hana,",
                "                        mariadb-columnstore (default postgres)",
                "  --modes LIST          comma-separated from: ROW, ROW+PART, ARROW,",
                "                        ARROW+PART (default ROW+PART)",
                "  --no-verify           skip verification step",
                "  --help, -h            show this help"));
    }

    private static List<Integer> parseModes(String modes, String[] allLabels) {
        List<Integer> selected = new ArrayList<>();
        for (String mode : modes.toUpperCase().split(",")) {
            String trimmed = mode.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            for (int m = 0; m < allLabels.length; m++) {
                if (allLabels[m].equals(trimmed)) {
                    selected.add(m);
                }
            }
        }
        if (selected.isEmpty()) {
            System.err.println("Unknown modes: " + modes
                    + ". Use: ROW,ROW+PART,ARROW,ARROW+PART");
            System.exit(1);
        }
        return selected;
    }

    private static List<String> parseSources(String sources) {
        List<String> result = new ArrayList<>();
        for (String s : sources.split(",")) {
            String trimmed = s.trim();
            if (!trimmed.isEmpty()) {
                result.add(trimmed);
            }
        }
        if (result.isEmpty()) {
            System.err.println("No sources specified.");
            System.exit(1);
        }
        return result;
    }

    private static String joinSelectedModes(List<Integer> selected,
            String[] allLabels) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < selected.size(); i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append(allLabels[selected.get(i)]);
        }
        return sb.toString();
    }

    private static void loadSourceData(SourceSetup setup,
            List<TableScenario> scenarios, int genBatchSize,
            int genPoolSize, long totalRows) throws Exception {
        int effectiveBatch = genBatchSize > 0
                ? genBatchSize
                : setup.loader.rowsPerInsertStatement();
        int effectivePool = setup.loader.loaderPoolSize() > 0
                ? setup.loader.loaderPoolSize()
                : genPoolSize;
        AtomicLong loaded = new AtomicLong();
        long startNanos = System.nanoTime();
        ScheduledExecutorService ticker = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "bench-progress");
            t.setDaemon(true);
            return t;
        });
        ticker.scheduleAtFixedRate(
                () -> printProgress(loaded.get(), totalRows, startNanos),
                5, 5, TimeUnit.SECONDS);

        ExecutorService pool = Executors.newFixedThreadPool(effectivePool);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (TableScenario s : scenarios) {
                futures.add(pool.submit(() -> {
                    try (Connection c = DriverManager.getConnection(
                            setup.container.getJdbcUrl(),
                            setup.container.getUsername(),
                            setup.container.getPassword())) {
                        if (setup.loader.requiresSetSchema()) {
                            try (java.sql.Statement st = c.createStatement()) {
                                st.execute("SET SCHEMA " + setup.schema);
                            }
                        }
                        setup.loader.createTable(c, s);
                        setup.loader.loadRows(c, s, effectiveBatch,
                                loaded::addAndGet);
                    }
                    return null;
                }));
            }
            for (Future<?> f : futures) {
                f.get();
            }
        } finally {
            pool.shutdown();
            ticker.shutdownNow();
        }
    }

    private static void printProgress(long loaded, long total, long startNanos) {
        double elapsedSec = (System.nanoTime() - startNanos) / 1e9;
        double rate = elapsedSec > 0 ? loaded / elapsedSec : 0;
        double pct = total > 0 ? 100.0 * loaded / total : 0;
        String eta = "-";
        if (rate > 0 && loaded < total) {
            long etaSec = (long) ((total - loaded) / rate);
            eta = etaSec + "s";
        }
        System.out.printf("  Loading: %,d / %,d (%.1f%%) @ %,.0f r/s  ETA %s%n",
                loaded, total, pct, rate, eta);
    }

    private static ModeResult runMode(String label, boolean useArrow,
            boolean usePartitions, List<TableScenario> scenarios,
            long totalRows, SourceSetup setup,
            LocalYdbTestContainer ydb,
            int batchSize, int poolSize, int fetchSize,
            boolean verify) throws Exception {

        System.out.printf("=== Import [%s] ===%n", label);

        YdbImporterRunner.Builder builder = YdbImporterRunner.builder()
                .source(setup.container, setup.sourceType)
                .ydb(ydb)
                .table(setup.schema, setup.tableName(scenarios.get(0)))
                .maxBatchRows(batchSize)
                .readerPoolSize(poolSize)
                .useArrow(useArrow)
                .usePartitions(usePartitions)
                .fetchSize(fetchSize)
                .targetPrefix(TARGET_PREFIX);
        for (int i = 1; i < scenarios.size(); i++) {
            builder.addTable(setup.schema, setup.tableName(scenarios.get(i)));
        }

        long importStart = System.currentTimeMillis();
        new YdbImporter(builder.buildConfig()).run();
        long importMs = System.currentTimeMillis() - importStart;

        System.out.printf("Import:       %,d rows in %.1fs (%s rows/s)%n",
                totalRows, importMs / 1000.0, rate(totalRows, importMs));

        long verifyMs = 0;
        List<Failure> failures = new ArrayList<>();
        if (verify) {
            long verifyStart = System.currentTimeMillis();
            try (YdbSchemaReader ydbReader = new YdbSchemaReader(
                    ydb.getConnectionString())) {
                for (TableScenario s : scenarios) {
                    String path = TARGET_PREFIX + "." + setup.schema
                            + "." + setup.tableName(s);
                    VerificationResult r = ScenarioVerifier.verify(
                            ydbReader, s, path, setup.loader);
                    if (!r.ok()) {
                        String summary = r.errorsSummary();
                        System.out.printf("  FAIL %s: %s%n",
                                s.tableName(), summary);
                        failures.add(new Failure(s.tableName(), summary));
                    }
                }
            }
            verifyMs = System.currentTimeMillis() - verifyStart;
            System.out.printf("Verify:       %,d rows in %.1fs (%s rows/s) %s%n",
                    totalRows, verifyMs / 1000.0,
                    rate(totalRows, verifyMs),
                    failures.isEmpty() ? "OK" : failures.size() + " FAILED");
        } else {
            System.out.printf("Verify:       skipped%n");
        }
        System.out.printf("Total [%s]:   %.1fs%n%n",
                label, (importMs + verifyMs) / 1000.0);

        return new ModeResult(label, importMs, verifyMs, failures);
    }

    private static String rate(long rows, long ms) {
        if (ms <= 0) {
            return "-";
        }
        return String.format("%,.0f", rows / (ms / 1000.0));
    }

    private static SourceSetup createSource(String name) throws Exception {
        switch (name.toLowerCase()) {
            case "clickhouse":
            case "ch":
                return SourceSetup.fromFixture(SourceDbProfile.clickhouse());
            case "mariadb":
            case "maria":
                return SourceSetup.fromFixture(SourceDbProfile.mariadb(DEFAULT_DB_NAME));
            case "mysql":
                return SourceSetup.fromFixture(SourceDbProfile.mysql(DEFAULT_DB_NAME));
            case "oracle":
                return SourceSetup.fromFixture(SourceDbProfile.oracle());
            case "db2":
                return SourceSetup.fromFixture(SourceDbProfile.db2());
            case "postgres":
            case "pg":
                return SourceSetup.fromFixture(SourceDbProfile.postgres(DEFAULT_DB_NAME));
            case "greenplum":
            case "gp":
                return SourceSetup.fromFixture(SourceDbProfile.greenplum());
            case "vertica":
                return SourceSetup.fromFixture(SourceDbProfile.vertica());
            case "hana":
                return SourceSetup.fromFixture(SourceDbProfile.hana());
            case "mariadb-columnstore":
            case "mariadb-cs":
                return createColumnStoreSetup();
            default:
                throw new IllegalArgumentException(
                        "Unknown source: " + name + ". Use: clickhouse,"
                                + " mariadb, mysql, oracle, db2,"
                                + " postgres, greenplum,"
                                + " vertica, hana, mariadb-columnstore");
        }
    }

    private static SourceSetup createColumnStoreSetup() throws Exception {
        ColumnStoreContainer cs = new ColumnStoreContainer();
        cs.start();
        return new SourceSetup(cs.getJdbcContainer(), SourceType.MARIADB,
                cs.getDatabaseName(), MariaDbColumnStoreLoader.INSTANCE,
                EnumSet.noneOf(Feature.class), true, cs::stop);
    }

    private static void printRowCounts(List<SourceRun> runs) {
        int width = 6;
        for (SourceRun r : runs) {
            if (r.source.length() > width) {
                width = r.source.length();
            }
        }
        System.out.printf("=== Row counts ===%n");
        long total = 0;
        for (SourceRun r : runs) {
            if (r.error != null) {
                System.out.printf("%-" + width + "s  %s%n",
                        r.source + ":", "(error)");
            } else {
                System.out.printf("%-" + width + "s  %,d%n",
                        r.source + ":", r.totalRows);
                total += r.totalRows;
            }
        }
        System.out.printf("%-" + width + "s  %,d%n%n", "Total:", total);
    }

    private static void printOverallSummary(List<SourceRun> runs) {
        System.out.printf("=== Overall Summary ===%n");
        System.out.printf("%-12s %-12s %10s %12s %10s %12s %10s %10s%n",
                "Source", "Mode", "Import(s)", "Import r/s",
                "Verify(s)", "Verify r/s", "Total(s)", "Result");
        for (SourceRun run : runs) {
            if (run.error != null) {
                System.out.printf("%-12s %-12s %10s %12s %10s %12s %10s %10s%n",
                        run.source, "-", "-", "-", "-", "-", "-", "ERROR");
                continue;
            }
            if (run.modes.isEmpty()) {
                System.out.printf("%-12s %-12s %10s %12s %10s %12s %10s %10s%n",
                        run.source, "-", "-", "-", "-", "-", "-", "no modes");
                continue;
            }
            for (ModeResult r : run.modes) {
                double impSec = r.importMs / 1000.0;
                double verSec = r.verifyMs / 1000.0;
                String verSecStr = verSec > 0
                        ? String.format("%10.1f", verSec)
                        : String.format("%10s", "-");
                String verRateStr = verSec > 0
                        ? String.format("%,12.0f", run.totalRows / verSec)
                        : String.format("%12s", "-");
                String result = r.failures.isEmpty()
                        ? "OK" : r.failures.size() + " fail";
                System.out.printf("%-12s %-12s %10.1f %12s %s %s %10.1f %10s%n",
                        run.source, r.label, impSec,
                        rate(run.totalRows, r.importMs),
                        verSecStr, verRateStr,
                        impSec + verSec, result);
            }
        }
    }

    private static void printFailures(List<SourceRun> runs) {
        boolean any = false;
        for (SourceRun run : runs) {
            if (run.error != null) {
                any = true;
                break;
            }
            for (ModeResult r : run.modes) {
                if (!r.failures.isEmpty()) {
                    any = true;
                    break;
                }
            }
            if (any) {
                break;
            }
        }
        if (!any) {
            return;
        }
        System.out.printf("%n=== Failures ===%n");
        for (SourceRun run : runs) {
            if (run.error != null) {
                System.out.printf("[%s] source error: %s%n",
                        run.source, run.error);
            }
            for (ModeResult r : run.modes) {
                for (Failure f : r.failures) {
                    System.out.printf("[%s][%s] %s: %s%n",
                            run.source, r.label, f.table,
                            f.summary.replace("\n", " | "));
                }
            }
        }
    }

    private static boolean anyError(List<SourceRun> runs) {
        for (SourceRun run : runs) {
            if (run.error != null) {
                return true;
            }
            for (ModeResult r : run.modes) {
                if (!r.failures.isEmpty()) {
                    return true;
                }
            }
        }
        return false;
    }

    private static void writeJson(int rows, int batchSize, int poolSize,
            int fetchSize, List<SourceRun> runs) throws IOException {
        Path dir = Paths.get("bench-results");
        Files.createDirectories(dir);
        int n = 1;
        Path out;
        do {
            out = dir.resolve("benchmark-" + n + ".json");
            n++;
        } while (Files.exists(out));

        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        sb.append("  \"params\": {\n");
        sb.append("    \"rows\": ").append(rows).append(",\n");
        sb.append("    \"batchSize\": ").append(batchSize).append(",\n");
        sb.append("    \"poolSize\": ").append(poolSize).append(",\n");
        sb.append("    \"fetchSize\": ").append(fetchSize).append("\n");
        sb.append("  },\n");
        sb.append("  \"runs\": [\n");
        for (int i = 0; i < runs.size(); i++) {
            appendRunJson(sb, runs.get(i));
            sb.append(i < runs.size() - 1 ? ",\n" : "\n");
        }
        sb.append("  ]\n");
        sb.append("}\n");

        Files.write(out, sb.toString().getBytes(StandardCharsets.UTF_8));
        System.out.printf("%nResults written to %s%n", out);
    }

    private static void appendRunJson(StringBuilder sb, SourceRun run) {
        sb.append("    {\n");
        sb.append("      \"source\": \"").append(jsonEscape(run.source)).append("\",\n");
        sb.append("      \"totalRows\": ").append(run.totalRows).append(",\n");
        sb.append("      \"loadMs\": ").append(run.loadMs).append(",\n");
        sb.append("      \"error\": ");
        if (run.error == null) {
            sb.append("null");
        } else {
            sb.append("\"").append(jsonEscape(run.error)).append("\"");
        }
        sb.append(",\n");
        sb.append("      \"modes\": [");
        if (run.modes.isEmpty()) {
            sb.append("]\n");
        } else {
            sb.append("\n");
            for (int i = 0; i < run.modes.size(); i++) {
                appendModeJson(sb, run.modes.get(i));
                sb.append(i < run.modes.size() - 1 ? ",\n" : "\n");
            }
            sb.append("      ]\n");
        }
        sb.append("    }");
    }

    private static void appendModeJson(StringBuilder sb, ModeResult r) {
        sb.append("        {\n");
        sb.append("          \"label\": \"").append(jsonEscape(r.label)).append("\",\n");
        sb.append("          \"importMs\": ").append(r.importMs).append(",\n");
        sb.append("          \"verifyMs\": ").append(r.verifyMs).append(",\n");
        sb.append("          \"failures\": [");
        if (r.failures.isEmpty()) {
            sb.append("]\n");
        } else {
            sb.append("\n");
            for (int j = 0; j < r.failures.size(); j++) {
                Failure f = r.failures.get(j);
                sb.append("            {\"table\": \"").append(jsonEscape(f.table))
                        .append("\", \"summary\": \"").append(jsonEscape(f.summary))
                        .append("\"}");
                sb.append(j < r.failures.size() - 1 ? ",\n" : "\n");
            }
            sb.append("          ]\n");
        }
        sb.append("        }");
    }

    private static String jsonEscape(String s) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"': sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                default:
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        return sb.toString();
    }

    private static final class Failure {
        final String table;
        final String summary;

        Failure(String table, String summary) {
            this.table = table;
            this.summary = summary;
        }
    }

    private static final class ModeResult {
        final String label;
        final long importMs;
        final long verifyMs;
        final List<Failure> failures;

        ModeResult(String label, long importMs, long verifyMs,
                   List<Failure> failures) {
            this.label = label;
            this.importMs = importMs;
            this.verifyMs = verifyMs;
            this.failures = failures;
        }
    }

    private static final class SourceRun {
        final String source;
        final List<ModeResult> modes = new ArrayList<>();
        long totalRows;
        long loadMs;
        String error;

        SourceRun(String source) {
            this.source = source;
        }
    }

    private static final class SourceSetup {
        final JdbcDatabaseContainer<?> container;
        final SourceType sourceType;
        final String schema;
        final DialectLoader loader;
        final Set<Feature> supported;
        final boolean alreadyStarted;
        final Runnable stopHook;

        SourceSetup(JdbcDatabaseContainer<?> container,
                    SourceType sourceType, String schema,
                    DialectLoader loader, Set<Feature> supported,
                    boolean alreadyStarted, Runnable stopHook) {
            this.container = container;
            this.sourceType = sourceType;
            this.schema = schema;
            this.loader = loader;
            this.supported = supported;
            this.alreadyStarted = alreadyStarted;
            this.stopHook = stopHook;
        }

        String tableName(TableScenario s) {
            return loader.effectiveTableName(s.tableName());
        }

        void stop() {
            if (stopHook != null) {
                stopHook.run();
            } else {
                container.stop();
            }
        }

        static SourceSetup fromFixture(SourceDbProfile f) {
            return new SourceSetup(f.container, f.sourceType, f.schema,
                    f.loader, f.supported, false, null);
        }
    }
}
