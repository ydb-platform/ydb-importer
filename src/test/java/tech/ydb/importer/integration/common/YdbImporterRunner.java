package tech.ydb.importer.integration.common;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.testcontainers.containers.JdbcDatabaseContainer;

import tech.ydb.importer.YdbImporter;
import tech.ydb.importer.config.ImporterConfig;
import tech.ydb.importer.config.SourceConfig;
import tech.ydb.importer.config.SourceType;
import tech.ydb.importer.config.TableOptions;
import tech.ydb.importer.config.TableRef;
import tech.ydb.importer.config.TargetConfig;
import tech.ydb.importer.config.TargetType;
import tech.ydb.importer.config.WorkerConfig;
import tech.ydb.importer.config.YdbAuthMode;

/** Sets up config and runs YdbImporter for integration tests */
public final class YdbImporterRunner {

    public static final String DEFAULT_TABLE_OPTIONS_NAME = "default";
    public static final String DEFAULT_TARGET_PREFIX = "test";

    private YdbImporterRunner() {
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private JdbcDatabaseContainer<?> sourceContainer;
        private SourceType sourceType;
        private LocalYdbTestContainer ydbContainer;
        private String schema;
        private String table;
        private final List<AdditionalTable> additionalTables = new ArrayList<>();
        private String targetPrefix = DEFAULT_TARGET_PREFIX;
        private Consumer<TableOptions> optionsCustomizer = opts -> { };
        private Consumer<TableRef> tableCustomizer = ref -> { };
        private int maxBatchRows = 1000;
        private int poolSize = 2;
        private int fetchSize = 10_000;
        private boolean usePartitions = true;
        private boolean useArrow = false;
        private String queryText;

        private static final class AdditionalTable {
            final String schema;
            final String table;
            final Consumer<TableRef> customizer;
            AdditionalTable(String schema, String table, Consumer<TableRef> customizer) {
                this.schema = schema;
                this.table = table;
                this.customizer = customizer;
            }
        }

        private Builder() {
        }

        public Builder source(JdbcDatabaseContainer<?> container, SourceType type) {
            this.sourceContainer = container;
            this.sourceType = type;
            return this;
        }

        public Builder ydb(LocalYdbTestContainer container) {
            this.ydbContainer = container;
            return this;
        }

        public Builder table(String schemaName, String tableName) {
            this.schema = schemaName;
            this.table = tableName;
            return this;
        }

        public Builder addTable(String schemaName, String tableName) {
            return addTable(schemaName, tableName, ref -> { });
        }

        public Builder addTable(String schemaName, String tableName,
                Consumer<TableRef> customizer) {
            this.additionalTables.add(new AdditionalTable(schemaName, tableName,
                    customizer == null ? ref -> { } : customizer));
            return this;
        }

        public Builder customizeTable(Consumer<TableRef> customizer) {
            this.tableCustomizer = customizer == null ? ref -> { } : customizer;
            return this;
        }

        public Builder targetPrefix(String prefix) {
            this.targetPrefix = prefix;
            return this;
        }

        public Builder customizeOptions(Consumer<TableOptions> customizer) {
            this.optionsCustomizer = customizer == null ? opts -> { } : customizer;
            return this;
        }

        public Builder maxBatchRows(int value) {
            this.maxBatchRows = value;
            return this;
        }

        public Builder poolSize(int value) {
            this.poolSize = value;
            return this;
        }

        public Builder fetchSize(int value) {
            this.fetchSize = value;
            return this;
        }

        public Builder usePartitions(boolean value) {
            this.usePartitions = value;
            return this;
        }

        public Builder useArrow(boolean value) {
            this.useArrow = value;
            return this;
        }

        public Builder queryText(String sql) {
            this.queryText = sql;
            return this;
        }

        public void run() throws Exception {
            ImporterConfig config = buildConfig();
            new YdbImporter(config).run();
        }

        public ImporterConfig buildConfig() {
            requireNonNull(sourceContainer, "source container");
            requireNonNull(sourceType, "source type");
            requireNonNull(ydbContainer, "ydb container");
            requireNonNull(schema, "schema");
            requireNonNull(table, "table");

            ImporterConfig config = new ImporterConfig();

            WorkerConfig workers = new WorkerConfig();
            workers.setReaderPoolSize(poolSize);
            workers.setWriterPoolSize(poolSize);
            workers.setBufferCount(poolSize);
            workers.setUseArrow(useArrow);
            config.setWorkers(workers);

            SourceConfig src = new SourceConfig();
            src.setType(sourceType);
            src.setClassName(sourceContainer.getDriverClassName());
            src.setJdbcUrl(sourceContainer.getJdbcUrl());
            src.setUserName(sourceContainer.getUsername());
            src.setPassword(sourceContainer.getPassword());
            src.setFetchSize(fetchSize);
            config.setSource(src);

            TargetConfig tgt = new TargetConfig();
            tgt.setType(TargetType.YDB);
            tgt.setAuthMode(YdbAuthMode.NONE);
            tgt.setConnectionString(ydbContainer.getConnectionString());
            tgt.setReplaceExisting(true);
            tgt.setLoadData(true);
            tgt.setMaxBatchRows(maxBatchRows);
            config.setTarget(tgt);

            TableOptions options = new TableOptions(
                    DEFAULT_TABLE_OPTIONS_NAME,
                    targetPrefix + ".${schema}.${table}");
            options.setBlobTemplate(targetPrefix + ".${schema}.${table}_${field}");
            options.setUseSourcePartitions(usePartitions);
            optionsCustomizer.accept(options);
            config.getOptionsMap().put(options.getName(), options);

            TableRef ref = new TableRef();
            ref.setOptions(options);
            ref.setSchema(schema);
            ref.setTable(table);
            if (queryText != null) {
                ref.setQueryText(queryText);
            }
            tableCustomizer.accept(ref);
            config.getTableRefs().add(ref);

            for (AdditionalTable extra : additionalTables) {
                TableRef extraRef = new TableRef();
                extraRef.setOptions(options);
                extraRef.setSchema(extra.schema);
                extraRef.setTable(extra.table);
                extra.customizer.accept(extraRef);
                config.getTableRefs().add(extraRef);
            }

            return config;
        }

        private static void requireNonNull(Object value, String name) {
            if (value == null) {
                throw new IllegalStateException(name + " must be set before run()");
            }
        }
    }
}
