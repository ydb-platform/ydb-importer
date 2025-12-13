package tech.ydb.importer.integration.dialects;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;

import tech.ydb.importer.config.SourceType;
import tech.ydb.importer.config.TableOptions;
import tech.ydb.importer.integration.DialectCase;
import tech.ydb.importer.integration.ExpectedRow;
import tech.ydb.importer.integration.ExpectedYdbColumn;
import tech.ydb.importer.integration.ExpectedYdbTable;
import tech.ydb.importer.integration.ImportCase;
import tech.ydb.importer.integration.ImportDialect;
import tech.ydb.importer.integration.SourceTableRef;
import tech.ydb.importer.integration.TableOptionsConfig;
import tech.ydb.table.values.DecimalType;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;

/**
 * ClickHouse dialect for integration matrix tests.
 */
@SuppressWarnings({"deprecation", "resource"})
public final class ClickHouseImportDialect implements ImportDialect {

    static final TableOptionsConfig DEFAULT_OPTIONS = new TableOptionsConfig(
            "default",
            "clickhouse.${schema}.${table}",
            "clickhouse.${schema}.${table}_${field}",
            TableOptions.CaseMode.ASIS,
            TableOptions.DateConv.DATE_NEW,
            TableOptions.DateConv.DATE_NEW,
            true,
            true
    );

    private static final String PUBLIC_SCHEMA = "public";

    @Override
    public String name() {
        return "clickhouse";
    }

    @Override
    public SourceType sourceType() {
        return SourceType.CLICKHOUSE;
    }

    @Override
    public String getJdbcDriverClass() {
        return "com.clickhouse.jdbc.ClickHouseDriver";
    }

    @Override
    public JdbcDatabaseContainer<?> createContainer() {
        return new ClickHouseContainerFixedDriver("clickhouse/clickhouse-server:25.3.10.19")
                .withEnv("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1");
    }

    @Override
    public List<DialectCase> cases() {
        return Arrays.asList(
                new DeclaredKeyLoadDataCase(),
                new NullableColumnsCase(),
                new NonMergeTreeEnginesCase(),
                new DecimalCase(),
                new SkipUnsupportedTypesCase(),
                new NoPrimaryKeyCase()
        );
    }

    private static final class ClickHouseContainerFixedDriver extends ClickHouseContainer {
        ClickHouseContainerFixedDriver(String dockerImageName) {
            super(dockerImageName);
        }

        @Override
        public String getDriverClassName() {
            return "com.clickhouse.jdbc.ClickHouseDriver";
        }
    }

    private static final class DeclaredKeyLoadDataCase implements DialectCase {
        private final ImportCase importCase;

        DeclaredKeyLoadDataCase() {
            SourceTableRef ref = new SourceTableRef(
                    PUBLIC_SCHEMA,
                    "tbl_with_pk_unique",
                    Collections.singletonList("record_uid"),
                    null,
                    "default"
            );

            ExpectedYdbTable expectedTable = new ExpectedYdbTable(
                    "clickhouse.public.tbl_with_pk_unique",
                    Arrays.asList(
                            new ExpectedYdbColumn("record_uid", PrimitiveType.Int64, false),
                            new ExpectedYdbColumn("item_code", PrimitiveType.Text, false),
                            new ExpectedYdbColumn("display_label", PrimitiveType.Text, true)
                    ),
                    Collections.singletonList("record_uid"),
                    Arrays.asList(
                            ExpectedRow.of(
                                    "record_uid", PrimitiveValue.newInt64(101),
                                    "item_code", PrimitiveValue.newText("ITEM-001"),
                                    "display_label", PrimitiveValue.newText("First Item")
                            ),
                            ExpectedRow.of(
                                    "record_uid", PrimitiveValue.newInt64(102),
                                    "item_code", PrimitiveValue.newText("ITEM-002"),
                                    "display_label", null
                            )
                    )
            );

            this.importCase = new ImportCase(
                    "declared_key_load_data",
                    "Declared table-ref key should be used as YDB PK",
                    true,
                    Collections.singletonList(DEFAULT_OPTIONS),
                    Collections.singletonList(ref),
                    Collections.singletonList(expectedTable)
            );
        }

        @Override
        public ImportCase getImportCase() {
            return importCase;
        }

        @Override
        public List<String> prepareSourceSql() {
            return Arrays.asList(
                    "CREATE DATABASE IF NOT EXISTS " + PUBLIC_SCHEMA,
                    "CREATE TABLE " + PUBLIC_SCHEMA + ".tbl_with_pk_unique (" +
                            "record_uid Int64, " +
                            "item_code String, " +
                            "display_label Nullable(String)" +
                            ") ENGINE = MergeTree ORDER BY record_uid",
                    "INSERT INTO " + PUBLIC_SCHEMA + ".tbl_with_pk_unique " +
                            "(record_uid, item_code, display_label) VALUES " +
                            "(101, 'ITEM-001', 'First Item')," +
                            "(102, 'ITEM-002', NULL)"
            );
        }

        @Override
        public List<String> cleanupSourceSql() {
            return Collections.singletonList(
                    "DROP TABLE IF EXISTS " + PUBLIC_SCHEMA + ".tbl_with_pk_unique"
            );
        }
    }

    private static final class NullableColumnsCase implements DialectCase {
        private final ImportCase importCase;

        NullableColumnsCase() {
            SourceTableRef ref = new SourceTableRef(
                    PUBLIC_SCHEMA,
                    "nullable_types",
                    Collections.singletonList("id"),
                    null,
                    "default"
            );

            ExpectedYdbTable expectedTable = new ExpectedYdbTable(
                    "clickhouse.public.nullable_types",
                    Arrays.asList(
                            new ExpectedYdbColumn("id", PrimitiveType.Int32, false),
                            new ExpectedYdbColumn("text", PrimitiveType.Text, true),
                            new ExpectedYdbColumn("amount", PrimitiveType.Int64, true),
                            new ExpectedYdbColumn("flag", PrimitiveType.Int32, true)
                    ),
                    Collections.singletonList("id"),
                    Arrays.asList(
                            ExpectedRow.of(
                                    "id", PrimitiveValue.newInt32(1),
                                    "text", PrimitiveValue.newText("not null"),
                                    "amount", PrimitiveValue.newInt64(100L),
                                    "flag", PrimitiveValue.newInt32(1)
                            ),
                            ExpectedRow.of(
                                    "id", PrimitiveValue.newInt32(2),
                                    "text", null,
                                    "amount", null,
                                    "flag", PrimitiveValue.newInt32(0)
                            )
                    )
            );

            this.importCase = new ImportCase(
                    "nullable_columns",
                    "Nullable ClickHouse columns should map to OPTIONAL YDB columns",
                    true,
                    Collections.singletonList(DEFAULT_OPTIONS),
                    Collections.singletonList(ref),
                    Collections.singletonList(expectedTable)
            );
        }

        @Override
        public ImportCase getImportCase() {
            return importCase;
        }

        @Override
        public List<String> prepareSourceSql() {
            return Arrays.asList(
                    "CREATE DATABASE IF NOT EXISTS " + PUBLIC_SCHEMA,
                    "CREATE TABLE " + PUBLIC_SCHEMA + ".nullable_types (" +
                            "id Int32, " +
                            "text Nullable(String), " +
                            "amount Nullable(Int64), " +
                            "flag Nullable(Int32)" +
                            ") ENGINE = MergeTree ORDER BY id",
                    "INSERT INTO " + PUBLIC_SCHEMA + ".nullable_types " +
                            "(id, text, amount, flag) VALUES " +
                            "(1, 'not null', 100, 1)," +
                            "(2, NULL, NULL, 0)"
            );
        }

        @Override
        public List<String> cleanupSourceSql() {
            return Collections.singletonList(
                    "DROP TABLE IF EXISTS " + PUBLIC_SCHEMA + ".nullable_types"
            );
        }
    }

    private static final class NonMergeTreeEnginesCase implements DialectCase {
        private final ImportCase importCase;

        NonMergeTreeEnginesCase() {
            List<SourceTableRef> tableRefs = Arrays.asList(
                    new SourceTableRef(PUBLIC_SCHEMA, "mem_metrics", Collections.singletonList("entry_id"), null, "default"),
                    new SourceTableRef(PUBLIC_SCHEMA, "log_events", Collections.singletonList("id"), null, "default")
            );

            List<ExpectedYdbTable> expectedTables = Arrays.asList(
                    new ExpectedYdbTable(
                            "clickhouse.public.mem_metrics",
                            Arrays.asList(
                                    new ExpectedYdbColumn("entry_id", PrimitiveType.Int64, false),
                                    new ExpectedYdbColumn("short_code", PrimitiveType.Text, false),
                                    new ExpectedYdbColumn("counter_value", PrimitiveType.Int64, false)
                            ),
                            Collections.singletonList("entry_id"),
                            Arrays.asList(
                                    ExpectedRow.of(
                                            "entry_id", PrimitiveValue.newInt64(1),
                                            "short_code", PrimitiveValue.newText("MC-001"),
                                            "counter_value", PrimitiveValue.newInt64(10)
                                    ),
                                    ExpectedRow.of(
                                            "entry_id", PrimitiveValue.newInt64(2),
                                            "short_code", PrimitiveValue.newText("MC-002"),
                                            "counter_value", PrimitiveValue.newInt64(20)
                                    )
                            )
                    ),
                    new ExpectedYdbTable(
                            "clickhouse.public.log_events",
                            Arrays.asList(
                                    new ExpectedYdbColumn("id", PrimitiveType.Int64, false),
                                    new ExpectedYdbColumn("category", PrimitiveType.Text, false),
                                    new ExpectedYdbColumn("value", PrimitiveType.Double, false)
                            ),
                            Collections.singletonList("id"),
                            Arrays.asList(
                                    ExpectedRow.of(
                                            "id", PrimitiveValue.newInt64(1),
                                            "category", PrimitiveValue.newText("A"),
                                            "value", PrimitiveValue.newDouble(10.5)
                                    ),
                                    ExpectedRow.of(
                                            "id", PrimitiveValue.newInt64(2),
                                            "category", PrimitiveValue.newText("B"),
                                            "value", PrimitiveValue.newDouble(20.25)
                                    )
                            )
                    )
            );

            this.importCase = new ImportCase(
                    "non_merge_tree_engines",
                    "Memory and Log engines should be readable and imported",
                    true,
                    Collections.singletonList(DEFAULT_OPTIONS),
                    tableRefs,
                    expectedTables
            );
        }

        @Override
        public ImportCase getImportCase() {
            return importCase;
        }

        @Override
        public List<String> prepareSourceSql() {
            return Arrays.asList(
                    "CREATE DATABASE IF NOT EXISTS " + PUBLIC_SCHEMA,
                    "CREATE TABLE " + PUBLIC_SCHEMA + ".mem_metrics (" +
                            "entry_id Int64, " +
                            "short_code String, " +
                            "counter_value Int64" +
                            ") ENGINE = Memory",
                    "INSERT INTO " + PUBLIC_SCHEMA + ".mem_metrics (entry_id, short_code, counter_value) VALUES " +
                            "(1, 'MC-001', 10)," +
                            "(2, 'MC-002', 20)",
                    "CREATE TABLE " + PUBLIC_SCHEMA + ".log_events (" +
                            "id Int64, " +
                            "category String, " +
                            "value Float64" +
                            ") ENGINE = Log",
                    "INSERT INTO " + PUBLIC_SCHEMA + ".log_events (id, category, value) VALUES " +
                            "(1, 'A', 10.5)," +
                            "(2, 'B', 20.25)"
            );
        }

        @Override
        public List<String> cleanupSourceSql() {
            return Arrays.asList(
                    "DROP TABLE IF EXISTS " + PUBLIC_SCHEMA + ".log_events",
                    "DROP TABLE IF EXISTS " + PUBLIC_SCHEMA + ".mem_metrics"
            );
        }
    }

    private static final class DecimalCase implements DialectCase {
        private final ImportCase importCase;

        DecimalCase() {
            SourceTableRef ref = new SourceTableRef(
                    PUBLIC_SCHEMA,
                    "decimal_metrics",
                    Collections.singletonList("region_id"),
                    null,
                    "default"
            );

            ExpectedYdbTable expectedTable = new ExpectedYdbTable(
                    "clickhouse.public.decimal_metrics",
                    Arrays.asList(
                            new ExpectedYdbColumn("region_id", PrimitiveType.Int32, false),
                            new ExpectedYdbColumn("city_name", PrimitiveType.Text, false),
                            new ExpectedYdbColumn("density", DecimalType.of(8, 3), false)
                    ),
                    Collections.singletonList("region_id"),
                    Arrays.asList(
                            ExpectedRow.of(
                                    "region_id", PrimitiveValue.newInt32(1),
                                    "city_name", PrimitiveValue.newText("Moscow"),
                                    "density", DecimalType.of(8, 3).newValue(new BigDecimal("4.942"))
                            ),
                            ExpectedRow.of(
                                    "region_id", PrimitiveValue.newInt32(2),
                                    "city_name", PrimitiveValue.newText("Novosibirsk"),
                                    "density", DecimalType.of(8, 3).newValue(new BigDecimal("0.000"))
                            )
                    )
            );

            this.importCase = new ImportCase(
                    "decimal_mapping",
                    "Decimal columns should map to YDB Decimal",
                    true,
                    Collections.singletonList(DEFAULT_OPTIONS),
                    Collections.singletonList(ref),
                    Collections.singletonList(expectedTable)
            );
        }

        @Override
        public ImportCase getImportCase() {
            return importCase;
        }

        @Override
        public List<String> prepareSourceSql() {
            return Arrays.asList(
                    "CREATE DATABASE IF NOT EXISTS " + PUBLIC_SCHEMA,
                    "CREATE TABLE " + PUBLIC_SCHEMA + ".decimal_metrics (" +
                            "region_id Int32, " +
                            "city_name String, " +
                            "density Decimal(8,3)" +
                            ") ENGINE = MergeTree ORDER BY region_id",
                    "INSERT INTO " + PUBLIC_SCHEMA + ".decimal_metrics (region_id, city_name, density) VALUES " +
                            "(1, 'Moscow', 4.942)," +
                            "(2, 'Novosibirsk', 0.000)"
            );
        }

        @Override
        public List<String> cleanupSourceSql() {
            return Collections.singletonList(
                    "DROP TABLE IF EXISTS " + PUBLIC_SCHEMA + ".decimal_metrics"
            );
        }
    }

    private static final class SkipUnsupportedTypesCase implements DialectCase {
        private final ImportCase importCase;

        SkipUnsupportedTypesCase() {
            SourceTableRef ref = new SourceTableRef(
                    PUBLIC_SCHEMA,
                    "skip_unsupported",
                    Collections.singletonList("id"),
                    null,
                    "default"
            );

            ExpectedYdbTable expectedTable = new ExpectedYdbTable(
                    "clickhouse.public.skip_unsupported",
                    Arrays.asList(
                            new ExpectedYdbColumn("id", PrimitiveType.Int64, false),
                            new ExpectedYdbColumn("title", PrimitiveType.Text, false)
                    ),
                    Collections.singletonList("id"),
                    Arrays.asList(
                            ExpectedRow.of(
                                    "id", PrimitiveValue.newInt64(1),
                                    "title", PrimitiveValue.newText("ok")
                            )
                    )
            );

            this.importCase = new ImportCase(
                    "skip_unsupported_types",
                    "Unsupported types (e.g., Array) should be skipped when enabled",
                    true,
                    Collections.singletonList(DEFAULT_OPTIONS),
                    Collections.singletonList(ref),
                    Collections.singletonList(expectedTable)
            );
        }

        @Override
        public ImportCase getImportCase() {
            return importCase;
        }

        @Override
        public List<String> prepareSourceSql() {
            return Arrays.asList(
                    "CREATE DATABASE IF NOT EXISTS " + PUBLIC_SCHEMA,
                    "CREATE TABLE " + PUBLIC_SCHEMA + ".skip_unsupported (" +
                            "id Int64, " +
                            "title String, " +
                            "tags Array(String)" +
                            ") ENGINE = MergeTree ORDER BY id",
                    "INSERT INTO " + PUBLIC_SCHEMA + ".skip_unsupported (id, title, tags) VALUES " +
                            "(1, 'ok', ['a','b'])"
            );
        }

        @Override
        public List<String> cleanupSourceSql() {
            return Collections.singletonList(
                    "DROP TABLE IF EXISTS " + PUBLIC_SCHEMA + ".skip_unsupported"
            );
        }
    }

    private static final class NoPrimaryKeyCase implements DialectCase {
        private final ImportCase importCase;

        NoPrimaryKeyCase() {
            SourceTableRef ref = new SourceTableRef(
                    PUBLIC_SCHEMA,
                    "no_pk_table",
                    Collections.emptyList(),
                    null,
                    "default"
            );

            ExpectedYdbTable expectedTable = new ExpectedYdbTable(
                    "clickhouse.public.no_pk_table",
                    Arrays.asList(
                            new ExpectedYdbColumn("ydb_synth_key", PrimitiveType.Text, true),
                            new ExpectedYdbColumn("session_id", PrimitiveType.Text, false),
                            new ExpectedYdbColumn("duration", PrimitiveType.Int32, true)
                    ),
                    Collections.singletonList("ydb_synth_key"),
                    Collections.emptyList()
            );

            this.importCase = new ImportCase(
                    "no_primary_key",
                    "Table without declared key",
                    false,
                    Collections.singletonList(DEFAULT_OPTIONS),
                    Collections.singletonList(ref),
                    Collections.singletonList(expectedTable)
            );
        }

        @Override
        public ImportCase getImportCase() {
            return importCase;
        }

        @Override
        public List<String> prepareSourceSql() {
            return Arrays.asList(
                    "CREATE DATABASE IF NOT EXISTS " + PUBLIC_SCHEMA,
                    "CREATE TABLE " + PUBLIC_SCHEMA + ".no_pk_table (" +
                            "session_id String, " +
                            "duration Nullable(Int32)" +
                            ") ENGINE = MergeTree ORDER BY tuple()",
                    "INSERT INTO " + PUBLIC_SCHEMA + ".no_pk_table (session_id, duration) VALUES " +
                            "('sess_001', 300)," +
                            "('sess_002', NULL)"
            );
        }

        @Override
        public List<String> cleanupSourceSql() {
            return Collections.singletonList(
                    "DROP TABLE IF EXISTS " + PUBLIC_SCHEMA + ".no_pk_table"
            );
        }
    }

}


