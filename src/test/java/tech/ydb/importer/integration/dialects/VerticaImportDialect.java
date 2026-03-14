package tech.ydb.importer.integration.dialects;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.github.dockerjava.api.model.Ulimit;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.utility.DockerImageName;

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
 * Vertica dialect for integration matrix tests.
 */
public final class VerticaImportDialect implements ImportDialect {

    static final TableOptionsConfig DEFAULT_OPTIONS = new TableOptionsConfig(
            "default",
            "vertica.${schema}.${table}",
            "vertica.${schema}.${table}_${field}",
            TableOptions.CaseMode.ASIS,
            TableOptions.DateConv.DATE_NEW,
            TableOptions.DateConv.DATE_NEW,
            true,
            true
    );

    private static final String PUBLIC_SCHEMA = "public";

    @Override
    public String name() {
        return "vertica";
    }

    @Override
    public SourceType sourceType() {
        return SourceType.VERTICA;
    }

    @Override
    public String getJdbcDriverClass() {
        return "com.vertica.jdbc.Driver";
    }

    @Override
    public JdbcDatabaseContainer<?> createContainer() {
        return new VerticaTestContainer("vertica-ce-bench:latest");
    }

    @Override
    public List<DialectCase> cases() {
        return Arrays.asList(
                new EnforcedPkLoadDataCase(),
                new NullableColumnsCase(),
                new NoPrimaryKeyCase(),
                new AdvisoryPkCase(),
                new EnabledUniqueCase(),
                new DecimalCase(),
                new PartitionedTableCase()
        );
    }

    private static final class VerticaTestContainer extends JdbcDatabaseContainer<VerticaTestContainer> {

        private static final int VERTICA_PORT = 5433;
        private static final String DB_NAME = "VMart";

        VerticaTestContainer(String dockerImageName) {
            super(DockerImageName.parse(dockerImageName));
            addExposedPort(VERTICA_PORT);
            withStartupTimeoutSeconds(300);
            withConnectTimeoutSeconds(120);
            withCreateContainerCmdModifier(cmd ->
                    cmd.getHostConfig().withUlimits(Collections.singletonList(
                            new Ulimit("nofile", 65536L, 65536L)
                    )));
        }

        @Override
        public String getDriverClassName() {
            return "com.vertica.jdbc.Driver";
        }

        @Override
        public String getJdbcUrl() {
            return "jdbc:vertica://" + getHost() + ":" + getMappedPort(VERTICA_PORT) + "/" + DB_NAME;
        }

        @Override
        public String getUsername() {
            return "dbadmin";
        }

        @Override
        public String getPassword() {
            return "";
        }

        @Override
        protected String getTestQueryString() {
            return "SELECT 1";
        }
    }

    private static final class EnforcedPkLoadDataCase implements DialectCase {
        private final ImportCase importCase;

        EnforcedPkLoadDataCase() {
            SourceTableRef ref = new SourceTableRef(
                    PUBLIC_SCHEMA,
                    "enforced_pk",
                    Collections.singletonList("record_uid"),
                    null,
                    "default"
            );

            ExpectedYdbTable expectedTable = new ExpectedYdbTable(
                    "vertica.public.enforced_pk",
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
                    "enforced_pk_load_data",
                    "Enforced PK should be used as YDB PK, data loaded",
                    true,
                    Collections.singletonList(DEFAULT_OPTIONS),
                    Collections.singletonList(ref),
                    Collections.singletonList(expectedTable)
            );
        }

        @Override
        public ImportCase getImportCase() { return importCase; }

        @Override
        public List<String> prepareSourceSql() {
            return Arrays.asList(
                    "CREATE TABLE " + PUBLIC_SCHEMA + ".enforced_pk ("
                            + "record_uid INT NOT NULL, "
                            + "item_code VARCHAR(20) NOT NULL, "
                            + "display_label VARCHAR(100), "
                            + "CONSTRAINT pk_enforced PRIMARY KEY (record_uid) ENABLED"
                            + ")",
                    "INSERT INTO " + PUBLIC_SCHEMA + ".enforced_pk "
                            + "(record_uid, item_code, display_label) VALUES "
                            + "(101, 'ITEM-001', 'First Item')",
                    "INSERT INTO " + PUBLIC_SCHEMA + ".enforced_pk "
                            + "(record_uid, item_code, display_label) VALUES "
                            + "(102, 'ITEM-002', NULL)"
            );
        }

        @Override
        public List<String> cleanupSourceSql() {
            return Collections.singletonList(
                    "DROP TABLE IF EXISTS " + PUBLIC_SCHEMA + ".enforced_pk CASCADE");
        }
    }

    private static final class NullableColumnsCase implements DialectCase {
        private final ImportCase importCase;

        NullableColumnsCase() {
            SourceTableRef ref = new SourceTableRef(PUBLIC_SCHEMA, "nullable_types",
                    Collections.singletonList("id"), null, "default");
            ExpectedYdbTable expectedTable = new ExpectedYdbTable(
                    "vertica.public.nullable_types",
                    Arrays.asList(
                            new ExpectedYdbColumn("id", PrimitiveType.Int64, false),
                            new ExpectedYdbColumn("text_col", PrimitiveType.Text, true),
                            new ExpectedYdbColumn("amount", PrimitiveType.Int64, true)
                    ),
                    Collections.singletonList("id"),
                    Arrays.asList(
                            ExpectedRow.of("id", PrimitiveValue.newInt64(1),
                                    "text_col", PrimitiveValue.newText("not null"),
                                    "amount", PrimitiveValue.newInt64(100L)),
                            ExpectedRow.of("id", PrimitiveValue.newInt64(2),
                                    "text_col", null, "amount", null)
                    )
            );
            this.importCase = new ImportCase("nullable_columns",
                    "Nullable Vertica columns should map to OPTIONAL YDB columns",
                    true, Collections.singletonList(DEFAULT_OPTIONS),
                    Collections.singletonList(ref), Collections.singletonList(expectedTable));
        }

        @Override
        public ImportCase getImportCase() { return importCase; }

        @Override
        public List<String> prepareSourceSql() {
            return Arrays.asList(
                    "CREATE TABLE " + PUBLIC_SCHEMA + ".nullable_types ("
                            + "id INT NOT NULL, text_col VARCHAR(100), amount INT, "
                            + "CONSTRAINT pk_nullable PRIMARY KEY (id) ENABLED)",
                    "INSERT INTO " + PUBLIC_SCHEMA
                            + ".nullable_types (id, text_col, amount) VALUES (1, 'not null', 100)",
                    "INSERT INTO " + PUBLIC_SCHEMA
                            + ".nullable_types (id, text_col, amount) VALUES (2, NULL, NULL)");
        }

        @Override
        public List<String> cleanupSourceSql() {
            return Collections.singletonList(
                    "DROP TABLE IF EXISTS " + PUBLIC_SCHEMA + ".nullable_types CASCADE");
        }
    }

    private static final class NoPrimaryKeyCase implements DialectCase {
        private final ImportCase importCase;

        NoPrimaryKeyCase() {
            SourceTableRef ref = new SourceTableRef(PUBLIC_SCHEMA, "no_pk_table",
                    Collections.emptyList(), null, "default");
            ExpectedYdbTable expectedTable = new ExpectedYdbTable(
                    "vertica.public.no_pk_table",
                    Arrays.asList(
                            new ExpectedYdbColumn("ydb_synth_key", PrimitiveType.Text, false),
                            new ExpectedYdbColumn("session_id", PrimitiveType.Text, false),
                            new ExpectedYdbColumn("duration", PrimitiveType.Int64, true)
                    ),
                    Collections.singletonList("ydb_synth_key"), Collections.emptyList());
            this.importCase = new ImportCase("no_primary_key",
                    "Table without PK or UNIQUE (tests synthetic key generation)",
                    false, Collections.singletonList(DEFAULT_OPTIONS),
                    Collections.singletonList(ref), Collections.singletonList(expectedTable));
        }

        @Override
        public ImportCase getImportCase() { return importCase; }

        @Override
        public List<String> prepareSourceSql() {
            return Arrays.asList(
                    "CREATE TABLE " + PUBLIC_SCHEMA
                            + ".no_pk_table (session_id VARCHAR(50) NOT NULL, duration INT)",
                    "INSERT INTO " + PUBLIC_SCHEMA
                            + ".no_pk_table (session_id, duration) VALUES ('sess_001', 300)",
                    "INSERT INTO " + PUBLIC_SCHEMA
                            + ".no_pk_table (session_id, duration) VALUES ('sess_002', NULL)");
        }

        @Override
        public List<String> cleanupSourceSql() {
            return Collections.singletonList(
                    "DROP TABLE IF EXISTS " + PUBLIC_SCHEMA + ".no_pk_table CASCADE");
        }
    }

    private static final class AdvisoryPkCase implements DialectCase {
        private final ImportCase importCase;

        AdvisoryPkCase() {
            SourceTableRef ref = new SourceTableRef(PUBLIC_SCHEMA, "advisory_pk",
                    Collections.emptyList(), null, "default");
            ExpectedYdbTable expectedTable = new ExpectedYdbTable(
                    "vertica.public.advisory_pk",
                    Arrays.asList(
                            new ExpectedYdbColumn("ydb_synth_key", PrimitiveType.Text, false),
                            new ExpectedYdbColumn("id", PrimitiveType.Int64, false),
                            new ExpectedYdbColumn("name", PrimitiveType.Text, true)
                    ),
                    Collections.singletonList("ydb_synth_key"), Collections.emptyList());
            this.importCase = new ImportCase("advisory_pk",
                    "Non-enforced (advisory) PK should be ignored, synthetic key used",
                    false, Collections.singletonList(DEFAULT_OPTIONS),
                    Collections.singletonList(ref), Collections.singletonList(expectedTable));
        }

        @Override
        public ImportCase getImportCase() { return importCase; }

        @Override
        public List<String> prepareSourceSql() {
            return Arrays.asList(
                    "CREATE TABLE " + PUBLIC_SCHEMA
                            + ".advisory_pk (id INT NOT NULL, name VARCHAR(100), PRIMARY KEY (id))",
                    "INSERT INTO " + PUBLIC_SCHEMA
                            + ".advisory_pk (id, name) VALUES (1, 'alpha')",
                    "INSERT INTO " + PUBLIC_SCHEMA
                            + ".advisory_pk (id, name) VALUES (2, 'beta')");
        }

        @Override
        public List<String> cleanupSourceSql() {
            return Collections.singletonList(
                    "DROP TABLE IF EXISTS " + PUBLIC_SCHEMA + ".advisory_pk CASCADE");
        }
    }

    private static final class EnabledUniqueCase implements DialectCase {
        private final ImportCase importCase;

        EnabledUniqueCase() {
            SourceTableRef ref = new SourceTableRef(PUBLIC_SCHEMA, "enabled_unique",
                    Collections.emptyList(), null, "default");
            ExpectedYdbTable expectedTable = new ExpectedYdbTable(
                    "vertica.public.enabled_unique",
                    Arrays.asList(
                            new ExpectedYdbColumn("entry_id", PrimitiveType.Int64, false),
                            new ExpectedYdbColumn("code", PrimitiveType.Text, false),
                            new ExpectedYdbColumn("value", PrimitiveType.Int64, true)
                    ),
                    Collections.singletonList("entry_id"),
                    Arrays.asList(
                            ExpectedRow.of("entry_id", PrimitiveValue.newInt64(1),
                                    "code", PrimitiveValue.newText("A-001"),
                                    "value", PrimitiveValue.newInt64(100)),
                            ExpectedRow.of("entry_id", PrimitiveValue.newInt64(2),
                                    "code", PrimitiveValue.newText("B-002"),
                                    "value", PrimitiveValue.newInt64(200))
                    ));
            this.importCase = new ImportCase("enabled_unique_as_key",
                    "Enabled UNIQUE constraint should be used as YDB PK when no PK",
                    true, Collections.singletonList(DEFAULT_OPTIONS),
                    Collections.singletonList(ref), Collections.singletonList(expectedTable));
        }

        @Override
        public ImportCase getImportCase() { return importCase; }

        @Override
        public List<String> prepareSourceSql() {
            return Arrays.asList(
                    "CREATE TABLE " + PUBLIC_SCHEMA + ".enabled_unique ("
                            + "entry_id INT NOT NULL, code VARCHAR(20) NOT NULL, value INT, "
                            + "CONSTRAINT uq_entry UNIQUE (entry_id) ENABLED)",
                    "INSERT INTO " + PUBLIC_SCHEMA
                            + ".enabled_unique (entry_id, code, value) VALUES (1, 'A-001', 100)",
                    "INSERT INTO " + PUBLIC_SCHEMA
                            + ".enabled_unique (entry_id, code, value) VALUES (2, 'B-002', 200)");
        }

        @Override
        public List<String> cleanupSourceSql() {
            return Collections.singletonList(
                    "DROP TABLE IF EXISTS " + PUBLIC_SCHEMA + ".enabled_unique CASCADE");
        }
    }

    private static final class DecimalCase implements DialectCase {
        private final ImportCase importCase;

        DecimalCase() {
            SourceTableRef ref = new SourceTableRef(PUBLIC_SCHEMA, "decimal_metrics",
                    Collections.singletonList("region_id"), null, "default");
            ExpectedYdbTable expectedTable = new ExpectedYdbTable(
                    "vertica.public.decimal_metrics",
                    Arrays.asList(
                            new ExpectedYdbColumn("region_id", PrimitiveType.Int64, false),
                            new ExpectedYdbColumn("city_name", PrimitiveType.Text, false),
                            new ExpectedYdbColumn("density", DecimalType.of(8, 3), true)
                    ),
                    Collections.singletonList("region_id"),
                    Arrays.asList(
                            ExpectedRow.of("region_id", PrimitiveValue.newInt64(1),
                                    "city_name", PrimitiveValue.newText("Moscow"),
                                    "density", DecimalType.of(8, 3).newValue(new BigDecimal("4.942"))),
                            ExpectedRow.of("region_id", PrimitiveValue.newInt64(2),
                                    "city_name", PrimitiveValue.newText("Novosibirsk"),
                                    "density", DecimalType.of(8, 3).newValue(new BigDecimal("0.000")))
                    ));
            this.importCase = new ImportCase("decimal_mapping",
                    "Decimal columns should map to YDB Decimal",
                    true, Collections.singletonList(DEFAULT_OPTIONS),
                    Collections.singletonList(ref), Collections.singletonList(expectedTable));
        }

        @Override
        public ImportCase getImportCase() { return importCase; }

        @Override
        public List<String> prepareSourceSql() {
            return Arrays.asList(
                    "CREATE TABLE " + PUBLIC_SCHEMA + ".decimal_metrics ("
                            + "region_id INT NOT NULL, city_name VARCHAR(100) NOT NULL, "
                            + "density NUMERIC(8,3), "
                            + "CONSTRAINT pk_decimal PRIMARY KEY (region_id) ENABLED)",
                    "INSERT INTO " + PUBLIC_SCHEMA
                            + ".decimal_metrics (region_id, city_name, density) VALUES (1, 'Moscow', 4.942)",
                    "INSERT INTO " + PUBLIC_SCHEMA
                            + ".decimal_metrics (region_id, city_name, density) VALUES (2, 'Novosibirsk', 0.000)");
        }

        @Override
        public List<String> cleanupSourceSql() {
            return Collections.singletonList(
                    "DROP TABLE IF EXISTS " + PUBLIC_SCHEMA + ".decimal_metrics CASCADE");
        }
    }

    private static final class PartitionedTableCase implements DialectCase {
        private final ImportCase importCase;

        PartitionedTableCase() {
            SourceTableRef ref = new SourceTableRef(PUBLIC_SCHEMA, "partitioned_sales",
                    Collections.singletonList("sale_id"), null, "default");
            ExpectedYdbTable expectedTable = new ExpectedYdbTable(
                    "vertica.public.partitioned_sales",
                    Arrays.asList(
                            new ExpectedYdbColumn("sale_id", PrimitiveType.Int64, false),
                            new ExpectedYdbColumn("region_id", PrimitiveType.Int64, false),
                            new ExpectedYdbColumn("amount", PrimitiveType.Int64, true)
                    ),
                    Collections.singletonList("sale_id"),
                    Arrays.asList(
                            ExpectedRow.of("sale_id", PrimitiveValue.newInt64(1),
                                    "region_id", PrimitiveValue.newInt64(1),
                                    "amount", PrimitiveValue.newInt64(100)),
                            ExpectedRow.of("sale_id", PrimitiveValue.newInt64(2),
                                    "region_id", PrimitiveValue.newInt64(1),
                                    "amount", PrimitiveValue.newInt64(200)),
                            ExpectedRow.of("sale_id", PrimitiveValue.newInt64(3),
                                    "region_id", PrimitiveValue.newInt64(2),
                                    "amount", PrimitiveValue.newInt64(150)),
                            ExpectedRow.of("sale_id", PrimitiveValue.newInt64(4),
                                    "region_id", PrimitiveValue.newInt64(3),
                                    "amount", PrimitiveValue.newInt64(300))
                    ));
            this.importCase = new ImportCase("partitioned_table",
                    "Partitioned table should be read via partition-aware parallel reading",
                    true, Collections.singletonList(DEFAULT_OPTIONS),
                    Collections.singletonList(ref), Collections.singletonList(expectedTable));
        }

        @Override
        public ImportCase getImportCase() { return importCase; }

        @Override
        public List<String> prepareSourceSql() {
            return Arrays.asList(
                    "CREATE TABLE " + PUBLIC_SCHEMA + ".partitioned_sales ("
                            + "sale_id INT NOT NULL, region_id INT NOT NULL, amount INT, "
                            + "CONSTRAINT pk_part_sales PRIMARY KEY (sale_id) ENABLED"
                            + ") PARTITION BY region_id",
                    "INSERT INTO " + PUBLIC_SCHEMA
                            + ".partitioned_sales (sale_id, region_id, amount) VALUES (1, 1, 100)",
                    "INSERT INTO " + PUBLIC_SCHEMA
                            + ".partitioned_sales (sale_id, region_id, amount) VALUES (2, 1, 200)",
                    "INSERT INTO " + PUBLIC_SCHEMA
                            + ".partitioned_sales (sale_id, region_id, amount) VALUES (3, 2, 150)",
                    "INSERT INTO " + PUBLIC_SCHEMA
                            + ".partitioned_sales (sale_id, region_id, amount) VALUES (4, 3, 300)");
        }

        @Override
        public List<String> cleanupSourceSql() {
            return Collections.singletonList(
                    "DROP TABLE IF EXISTS " + PUBLIC_SCHEMA + ".partitioned_sales CASCADE");
        }
    }
}
