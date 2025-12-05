package tech.ydb.importer.integration;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

import tech.ydb.importer.config.SourceType;
import tech.ydb.importer.config.TableOptions;

import java.math.BigDecimal;

import tech.ydb.table.values.DecimalType;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;


public final class PostgresImportDialect implements ImportDialect {
    
    private static final TableOptionsConfig DEFAULT_OPTIONS = new TableOptionsConfig(
        "default",
        "${schema}.${table}",
        "${schema}.${table}_${field}",
        TableOptions.CaseMode.ASIS,
        TableOptions.DateConv.DATE_NEW,
        TableOptions.DateConv.DATE_NEW,
        true,
        true
    );
    
    private static final String PUBLIC_SCHEMA = "public";

    @Override
    public String name() {
        return "postgres";
    }

    @Override
    public SourceType sourceType() {
        return SourceType.POSTGRESQL;
    }

    @Override
    public String getJdbcDriverClass() {
        return "org.postgresql.Driver";
    }

    @Override
    public JdbcDatabaseContainer<?> createContainer() {
        return new PostgreSQLContainer("postgres:17.5")
                .withDatabaseName("import-it-db")
                .withUsername("test")
                .withPassword("test");
    }

    @Override
    public List<DialectCase> cases() {
        return Arrays.asList(
            new CompositePkCase(),
            new NullableTypesCase(),
            new DuplicateRowsCase(),
            new NoPrimaryKeyCase(),
            new SchemaOnlyCases(),
            new PrimaryKeyWithUniqueCase(),
            new UniquePreferFewerColumnsCase(),
            new UniqueSortedColumnNamesCase()
        );
    }

    private static final class PrimaryKeyWithUniqueCase implements DialectCase {
        private final ImportCase importCase;

        PrimaryKeyWithUniqueCase() {
            SourceTableRef ref = new SourceTableRef(
                PUBLIC_SCHEMA,
                "tbl_with_pk_unique",
                Collections.emptyList(),
                null,
                "default"
            );

            ExpectedYdbTable expectedTable = new ExpectedYdbTable(
                "public.tbl_with_pk_unique",
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
                        "display_label", PrimitiveValue.newText("Second Item")
                    )
                )
            );

            this.importCase = new ImportCase(
                "pk_with_unique",
                "Table with PRIMARY KEY and UNIQUE constraint (should use onlyPRIMARY KEY)",
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
        public void prepareSourceData(Connection connection) throws Exception {
            try (Statement st = connection.createStatement()) {
                st.execute("CREATE SCHEMA IF NOT EXISTS public");
                st.execute(
                    "CREATE TABLE public.tbl_with_pk_unique (" +
                    "record_uid      BIGINT PRIMARY KEY," +
                    "item_code       VARCHAR(20) NOT NULL UNIQUE," +
                    "display_label   VARCHAR(100) NULL" +
                    ")"
                );
                st.execute(
                    "INSERT INTO public.tbl_with_pk_unique " +
                    "(record_uid, item_code, display_label) VALUES " +
                    "(101, 'ITEM-001', 'First Item')," +
                    "(102, 'ITEM-002', 'Second Item')"
                );
            }
        }
    }

    private static final class UniquePreferFewerColumnsCase implements DialectCase {
        private final ImportCase importCase;

        UniquePreferFewerColumnsCase() {
            SourceTableRef ref = new SourceTableRef(
                PUBLIC_SCHEMA,
                "tbl_unique_columns",
                Collections.emptyList(),
                null,
                "default"
            );

            ExpectedYdbTable expectedTable = new ExpectedYdbTable(
                "public.tbl_unique_columns",
                Arrays.asList(
                    new ExpectedYdbColumn("entry_id", PrimitiveType.Int64, false),
                    new ExpectedYdbColumn("short_code", PrimitiveType.Text, false),
                    new ExpectedYdbColumn("sector_uid", PrimitiveType.Text, false),
                    new ExpectedYdbColumn("node_uid", PrimitiveType.Text, false),
                    new ExpectedYdbColumn("counter_value", PrimitiveType.Int64, true)
                ),
                Collections.singletonList("node_uid"),
                Arrays.asList(
                    ExpectedRow.of(
                        "entry_id", PrimitiveValue.newInt64(1),
                        "short_code", PrimitiveValue.newText("SC-001"),
                        "sector_uid", PrimitiveValue.newText("SECTOR-A"),
                        "node_uid", PrimitiveValue.newText("NODE-01"),
                        "counter_value", PrimitiveValue.newInt64(100)
                    ),
                    ExpectedRow.of(
                        "entry_id", PrimitiveValue.newInt64(2),
                        "short_code", PrimitiveValue.newText("SC-002"),
                        "sector_uid", PrimitiveValue.newText("SECTOR-B"),
                        "node_uid", PrimitiveValue.newText("NODE-02"),
                        "counter_value", PrimitiveValue.newInt64(200)
                    )
                )
            );

            this.importCase = new ImportCase(
                "unique_prefer_fewer_columns",
                "Table with multiple UNIQUE constraints (should choose the one with fewer columns)",
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
        public void prepareSourceData(Connection connection) throws Exception {
            try (Statement st = connection.createStatement()) {
                st.execute("CREATE SCHEMA IF NOT EXISTS public");
                st.execute(
                    "CREATE TABLE public.tbl_unique_columns (" +
                    "entry_id       BIGINT NOT NULL," +
                    "short_code     VARCHAR(10) NOT NULL," +
                    "sector_uid     VARCHAR(8) NOT NULL," +
                    "node_uid       VARCHAR(8) NOT NULL UNIQUE," +
                    "counter_value  BIGINT NULL," +
                    "UNIQUE (short_code, sector_uid)" +
                    ")"
                );
                st.execute(
                    "INSERT INTO public.tbl_unique_columns " +
                    "(entry_id, short_code, sector_uid, node_uid, counter_value) VALUES " +
                    "(1, 'SC-001', 'SECTOR-A', 'NODE-01', 100)," +
                    "(2, 'SC-002', 'SECTOR-B', 'NODE-02', 200)"
                );
            }
        }
    }


    private static final class UniqueSortedColumnNamesCase implements DialectCase {
        private final ImportCase importCase;

        UniqueSortedColumnNamesCase() {
            SourceTableRef ref = new SourceTableRef(
                PUBLIC_SCHEMA,
                "tbl_sorted_unique",
                Collections.emptyList(),
                null,
                "default"
            );

            ExpectedYdbTable expectedTable = new ExpectedYdbTable(
                "public.tbl_sorted_unique",
                Arrays.asList(
                    new ExpectedYdbColumn("row_num", PrimitiveType.Int64, false),
                    new ExpectedYdbColumn("field_a", PrimitiveType.Text, false),
                    new ExpectedYdbColumn("field_b", PrimitiveType.Text, false),
                    new ExpectedYdbColumn("field_z", PrimitiveType.Text, false)               ),
                Arrays.asList("field_a", "field_b"),
                Arrays.asList(
                    ExpectedRow.of(
                        "row_num", PrimitiveValue.newInt64(1),
                        "field_a", PrimitiveValue.newText("val1"),
                        "field_b", PrimitiveValue.newText("val2"),
                        "field_z", PrimitiveValue.newText("val3")
                    ),
                    ExpectedRow.of(
                        "row_num", PrimitiveValue.newInt64(2),
                        "field_a", PrimitiveValue.newText("val4"),
                        "field_b", PrimitiveValue.newText("val5"),
                        "field_z", PrimitiveValue.newText("val6")
                    )
                )
            );

            this.importCase = new ImportCase(
                "unique_sorted_column_names",
                "Table with multiple UNIQUE constraints with same column count (should use sorted column names)",
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
        public void prepareSourceData(Connection connection) throws Exception {
            try (Statement st = connection.createStatement()) {
                st.execute("CREATE SCHEMA IF NOT EXISTS public");
                st.execute(
                    "CREATE TABLE public.tbl_sorted_unique (" +
                    "row_num      BIGINT NOT NULL," +
                    "field_a      VARCHAR(10) NOT NULL," +
                    "field_b      VARCHAR(10) NOT NULL," +
                    "field_z      VARCHAR(10) NOT NULL," +
                    "UNIQUE (field_z, field_a)," +
                    "UNIQUE (field_a, field_b)," +
                    "UNIQUE (field_a, field_z)" +
                    ")"
                );
                st.execute(
                    "INSERT INTO public.tbl_sorted_unique " +
                    "(row_num, field_a, field_b, field_z) VALUES " +
                    "(1, 'val1', 'val2', 'val3')," +
                    "(2, 'val4', 'val5', 'val6')"
                );
            }
        }
    }

    private static final class CompositePkCase implements DialectCase {
        private final ImportCase importCase;

        CompositePkCase() {
            SourceTableRef ref = new SourceTableRef(
                PUBLIC_SCHEMA,
                "composite_pk_table",
                Arrays.asList("region_id", "city_id"),
                null,
                "default"
            );

            ExpectedYdbTable expectedTable = new ExpectedYdbTable(
                "public.composite_pk_table",
                Arrays.asList(
                    new ExpectedYdbColumn("region_id", PrimitiveType.Int32, false),
                    new ExpectedYdbColumn("city_id", PrimitiveType.Int32, false),
                    new ExpectedYdbColumn("city_name", PrimitiveType.Text, false),
                    new ExpectedYdbColumn("density", DecimalType.of(8, 3), true)
                ),
                Arrays.asList("region_id", "city_id"),
                Arrays.asList(
                    ExpectedRow.of(
                        "region_id", PrimitiveValue.newInt32(1),
                        "city_id", PrimitiveValue.newInt32(101),
                        "city_name", PrimitiveValue.newText("Moscow"),
                        "density", DecimalType.of(8, 3).newValue(new BigDecimal("4.942"))
                    ),
                    ExpectedRow.of(
                        "region_id", PrimitiveValue.newInt32(2),
                        "city_id", PrimitiveValue.newInt32(201),
                        "city_name", PrimitiveValue.newText("Novosibirsk"),
                        "density", DecimalType.of(8, 3).newValue(new BigDecimal("0.000"))
                    )
                )
            );

            this.importCase = new ImportCase(
                "composite_pk_decimal",
                "Table with composite PK and decimal types",
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
        public void prepareSourceData(Connection connection) throws Exception {
            try (Statement st = connection.createStatement()) {
                st.execute("CREATE SCHEMA IF NOT EXISTS public");
                st.execute(
                    "CREATE TABLE public.composite_pk_table (" +
                    "region_id   INTEGER      NOT NULL," +
                    "city_id     INTEGER      NOT NULL," +
                    "city_name   VARCHAR(100) NOT NULL," +
                    "density     NUMERIC(8,3) NULL," +
                    "PRIMARY KEY (region_id, city_id)" +
                    ")"
                );
                st.execute(
                    "INSERT INTO public.composite_pk_table " +
                    "(region_id, city_id, city_name, density) VALUES " +
                    "(1, 101, 'Moscow', 4.942)," +
                    "(2, 201, 'Novosibirsk', NULL)"
                );
            }
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
                "public.no_pk_table",
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
                "Table without primary key (tests synthetic key generation)",
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
        public void prepareSourceData(Connection connection) throws Exception {
            try (Statement st = connection.createStatement()) {
                st.execute("CREATE SCHEMA IF NOT EXISTS public");
                st.execute(
                    "CREATE TABLE public.no_pk_table (" +
                    "session_id VARCHAR(50)  NOT NULL," +
                    "duration   INTEGER      NULL" +
                    ")"
                );
                st.execute(
                    "INSERT INTO public.no_pk_table " +
                    "(session_id, duration) VALUES " +
                    "('sess_001', 300)," +
                    "('sess_002', NULL)"
                );
            }
        }
    }

    private static final class DuplicateRowsCase implements DialectCase {
        private final ImportCase importCase;

        DuplicateRowsCase() {
            SourceTableRef ref = new SourceTableRef(
                PUBLIC_SCHEMA,
                "duplicate_rows",
                Collections.singletonList("id"),
                null,
                "default"
            );

            ExpectedYdbTable expectedTable = new ExpectedYdbTable(
                "public.duplicate_rows",
                Arrays.asList(
                    new ExpectedYdbColumn("id", PrimitiveType.Int32, false),
                    new ExpectedYdbColumn("category", PrimitiveType.Text, false),
                    new ExpectedYdbColumn("value", PrimitiveType.Double, false)
                ),
                Collections.singletonList("id"),
                Arrays.asList(
                    ExpectedRow.of(
                        "id", PrimitiveValue.newInt32(1),
                        "category", PrimitiveValue.newText("A"),
                        "value", PrimitiveValue.newDouble(10.5)
                    ),
                    ExpectedRow.of(
                        "id", PrimitiveValue.newInt32(2),
                        "category", PrimitiveValue.newText("B"),
                        "value", PrimitiveValue.newDouble(20.3)
                    ),
                    ExpectedRow.of(
                        "id", PrimitiveValue.newInt32(3),
                        "category", PrimitiveValue.newText("A"),
                        "value", PrimitiveValue.newDouble(10.5)
                    )
                )
            );

            this.importCase = new ImportCase(
                "duplicate_rows",
                "Table with duplicate non-PK values",
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
        public void prepareSourceData(Connection connection) throws Exception {
            try (Statement st = connection.createStatement()) {
                st.execute("CREATE SCHEMA IF NOT EXISTS public");
                st.execute(
                    "CREATE TABLE public.duplicate_rows (" +
                    "id         INTEGER PRIMARY KEY," +
                    "category   VARCHAR(10) NOT NULL," +
                    "value      DOUBLE PRECISION NOT NULL" +
                    ")"
                );
                st.execute(
                    "INSERT INTO public.duplicate_rows (id, category, value) VALUES " +
                    "(1, 'A', 10.5)," +
                    "(2, 'B', 20.3)," +
                    "(3, 'A', 10.5)"
                );
            }
        }
    }

    private static final class NullableTypesCase implements DialectCase {
        private final ImportCase importCase;

        NullableTypesCase() {
            SourceTableRef ref = new SourceTableRef(
                PUBLIC_SCHEMA,
                "nullable_types",
                Collections.singletonList("id"),
                null,
                "default"
            );

            ExpectedYdbTable expectedTable = new ExpectedYdbTable(
                "public.nullable_types",
                Arrays.asList(
                    new ExpectedYdbColumn("id", PrimitiveType.Int32, false),
                    new ExpectedYdbColumn("text_nullable", PrimitiveType.Text, true),
                    new ExpectedYdbColumn("int_nullable", PrimitiveType.Int64, true),
                    new ExpectedYdbColumn("bool_nullable", PrimitiveType.Bool, true)
                ),
                Collections.singletonList("id"),
                Arrays.asList(
                    ExpectedRow.of(
                        "id", PrimitiveValue.newInt32(1),
                        "text_nullable", PrimitiveValue.newText("not null"),
                        "int_nullable", PrimitiveValue.newInt64(100L),
                        "bool_nullable", PrimitiveValue.newBool(true)
                    ),
                    ExpectedRow.of(
                        "id", PrimitiveValue.newInt32(2),
                        "text_nullable", PrimitiveValue.newText(""),
                        "int_nullable", PrimitiveValue.newInt64(0L),
                        "bool_nullable", PrimitiveValue.newBool(false)
                    )
                )
            );

            this.importCase = new ImportCase(
                "nullable_types",
                "Table with nullable columns",
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
        public void prepareSourceData(Connection connection) throws Exception {
            try (Statement st = connection.createStatement()) {
                st.execute("CREATE SCHEMA IF NOT EXISTS public");
                st.execute(
                    "CREATE TABLE public.nullable_types (" +
                    "id           INTEGER PRIMARY KEY," +
                    "text_nullable VARCHAR(100) NULL," +
                    "int_nullable  BIGINT       NULL," +
                    "bool_nullable BOOLEAN      NULL" +
                    ")"
                );
                st.execute(
                    "INSERT INTO public.nullable_types " +
                    "(id, text_nullable, int_nullable, bool_nullable) VALUES " +
                    "(1, 'not null', 100, true)," +
                    "(2, NULL, NULL, NULL)"
                );
            }
        }
    }

    private static final class SchemaOnlyCases implements DialectCase {
        private final ImportCase importCase;

        SchemaOnlyCases() {
            List<SourceTableRef> tableRefs = Arrays.asList(
                new SourceTableRef(PUBLIC_SCHEMA, "lab", Collections.emptyList(), null, "default"),
                new SourceTableRef(PUBLIC_SCHEMA, "experiment_run", Collections.singletonList("id"), null, "default")
            );

            List<ExpectedYdbTable> expectedTables = Arrays.asList(
                new ExpectedYdbTable(
                    "public.lab",
                    Arrays.asList(
                        new ExpectedYdbColumn("id", PrimitiveType.Int64, false),
                        new ExpectedYdbColumn("code", PrimitiveType.Text, false),
                        new ExpectedYdbColumn("title", PrimitiveType.Text, false),
                        new ExpectedYdbColumn("created_at", PrimitiveType.Timestamp64, false)
                    ),
                    Arrays.asList("id"),
                    Collections.emptyList()
                ),
                new ExpectedYdbTable(
                    "public.experiment_run",
                    Arrays.asList(
                        new ExpectedYdbColumn("id", PrimitiveType.Int64, false),
                        new ExpectedYdbColumn("protocol_id", PrimitiveType.Int64, false),
                        new ExpectedYdbColumn("started_at", PrimitiveType.Timestamp64, false),
                        new ExpectedYdbColumn("finished_at", PrimitiveType.Timestamp64, true),
                        new ExpectedYdbColumn("status", PrimitiveType.Text, false)
                    ),
                    Collections.singletonList("id"),
                    Collections.emptyList()
                )
            );

            this.importCase = new ImportCase(
                "schema_only",
                "Schema-only tests for multiple table types",
                false,
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
        public void prepareSourceData(Connection connection) throws Exception {
            try (Statement st = connection.createStatement()) {
                st.execute("CREATE SCHEMA IF NOT EXISTS public");
                
                st.execute(
                    "CREATE TABLE public.lab (" +
                    "id         BIGINT PRIMARY KEY," +
                    "code       VARCHAR(32)  NOT NULL UNIQUE," +
                    "title      VARCHAR(200) NOT NULL," +
                    "created_at TIMESTAMP    NOT NULL" +
                    ")"
                );
                st.execute(
                    "INSERT INTO public.lab (id, code, title, created_at) VALUES " +
                    "(1, 'LAB-A', 'Test protocol A', CURRENT_TIMESTAMP)"
                );
                
                st.execute(
                    "CREATE TABLE public.experiment_run (" +
                    "id          BIGINT PRIMARY KEY," +
                    "protocol_id BIGINT      NOT NULL," +
                    "started_at  TIMESTAMP   NOT NULL," +
                    "finished_at TIMESTAMP   NULL," +
                    "status      VARCHAR(16) NOT NULL" +
                    ")"
                );
                st.execute(
                    "INSERT INTO public.experiment_run " +
                    "(id, protocol_id, started_at, finished_at, status) VALUES " +
                    "(100, 1, CURRENT_TIMESTAMP, NULL, 'running')"
                );
            }
        }
    }
}