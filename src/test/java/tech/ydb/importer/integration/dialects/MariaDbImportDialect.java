package tech.ydb.importer.integration.dialects;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.mariadb.MariaDBContainer;

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
 * MariaDB dialect for integration matrix tests.
 */
public final class MariaDbImportDialect implements ImportDialect {

    static final TableOptionsConfig DEFAULT_OPTIONS = new TableOptionsConfig(
            "default",
            "mariadb.${schema}.${table}",
            "mariadb.${schema}.${table}_${field}",
            TableOptions.CaseMode.ASIS,
            TableOptions.DateConv.DATE_NEW,
            TableOptions.DateConv.DATE_NEW,
            true,
            true
    );

    private static final String TEST_SCHEMA = "import_test";

    @Override
    public String name() {
        return "mariadb";
    }

    @Override
    public SourceType sourceType() {
        return SourceType.MARIADB;
    }

    @Override
    public String getJdbcDriverClass() {
        return "org.mariadb.jdbc.Driver";
    }

    @Override
    @SuppressWarnings("resource")
    public JdbcDatabaseContainer<?> createContainer() {
        return new MariaDBContainer("mariadb:11.7")
                .withDatabaseName(TEST_SCHEMA)
                .withUsername("test")
                .withPassword("test");
    }

    @Override
    public List<DialectCase> cases() {
        return Arrays.asList(
                new DeclaredKeyLoadDataCase(),
                new NullableColumnsCase(),
                new NoPrimaryKeyCase(),
                new UniqueConstraintAsKeyCase(),
                new DecimalCase()
        );
    }

    private static final class DeclaredKeyLoadDataCase implements DialectCase {
        private final ImportCase importCase;

        DeclaredKeyLoadDataCase() {
            SourceTableRef ref = new SourceTableRef(
                    TEST_SCHEMA,
                    "tbl_declared_key",
                    Collections.singletonList("record_uid"),
                    null,
                    "default"
            );

            ExpectedYdbTable expectedTable = new ExpectedYdbTable(
                    "mariadb.import_test.tbl_declared_key",
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
                    "CREATE TABLE " + TEST_SCHEMA + ".tbl_declared_key (" +
                            "record_uid BIGINT NOT NULL, " +
                            "item_code VARCHAR(20) NOT NULL, " +
                            "display_label VARCHAR(100) NULL, " +
                            "PRIMARY KEY (record_uid)" +
                            ") ENGINE=InnoDB",
                    "INSERT INTO " + TEST_SCHEMA + ".tbl_declared_key " +
                            "(record_uid, item_code, display_label) VALUES " +
                            "(101, 'ITEM-001', 'First Item')," +
                            "(102, 'ITEM-002', NULL)"
            );
        }

        @Override
        public List<String> cleanupSourceSql() {
            return Collections.singletonList(
                    "DROP TABLE IF EXISTS " + TEST_SCHEMA + ".tbl_declared_key"
            );
        }
    }

    private static final class NullableColumnsCase implements DialectCase {
        private final ImportCase importCase;

        NullableColumnsCase() {
            SourceTableRef ref = new SourceTableRef(
                    TEST_SCHEMA,
                    "nullable_types",
                    Collections.singletonList("id"),
                    null,
                    "default"
            );

            ExpectedYdbTable expectedTable = new ExpectedYdbTable(
                    "mariadb.import_test.nullable_types",
                    Arrays.asList(
                            new ExpectedYdbColumn("id", PrimitiveType.Int32, false),
                            new ExpectedYdbColumn("text_col", PrimitiveType.Text, true),
                            new ExpectedYdbColumn("amount", PrimitiveType.Int64, true),
                            new ExpectedYdbColumn("score", PrimitiveType.Int32, true)
                    ),
                    Collections.singletonList("id"),
                    Arrays.asList(
                            ExpectedRow.of(
                                    "id", PrimitiveValue.newInt32(1),
                                    "text_col", PrimitiveValue.newText("not null"),
                                    "amount", PrimitiveValue.newInt64(100L),
                                    "score", PrimitiveValue.newInt32(42)
                            ),
                            ExpectedRow.of(
                                    "id", PrimitiveValue.newInt32(2),
                                    "text_col", null,
                                    "amount", null,
                                    "score", null
                            )
                    )
            );

            this.importCase = new ImportCase(
                    "nullable_columns",
                    "Nullable MariaDB columns should map to OPTIONAL YDB columns",
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
                    "CREATE TABLE " + TEST_SCHEMA + ".nullable_types (" +
                            "id INT NOT NULL PRIMARY KEY, " +
                            "text_col VARCHAR(100) NULL, " +
                            "amount BIGINT NULL, " +
                            "score INT NULL" +
                            ") ENGINE=InnoDB",
                    "INSERT INTO " + TEST_SCHEMA + ".nullable_types " +
                            "(id, text_col, amount, score) VALUES " +
                            "(1, 'not null', 100, 42)," +
                            "(2, NULL, NULL, NULL)"
            );
        }

        @Override
        public List<String> cleanupSourceSql() {
            return Collections.singletonList(
                    "DROP TABLE IF EXISTS " + TEST_SCHEMA + ".nullable_types"
            );
        }
    }

    private static final class NoPrimaryKeyCase implements DialectCase {
        private final ImportCase importCase;

        NoPrimaryKeyCase() {
            SourceTableRef ref = new SourceTableRef(
                    TEST_SCHEMA,
                    "no_pk_table",
                    Collections.emptyList(),
                    null,
                    "default"
            );

            ExpectedYdbTable expectedTable = new ExpectedYdbTable(
                    "mariadb.import_test.no_pk_table",
                    Arrays.asList(
                            new ExpectedYdbColumn("ydb_synth_key", PrimitiveType.Text, false),
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
        public List<String> prepareSourceSql() {
            return Arrays.asList(
                    "CREATE TABLE " + TEST_SCHEMA + ".no_pk_table (" +
                            "session_id VARCHAR(50) NOT NULL, " +
                            "duration INT NULL" +
                            ") ENGINE=InnoDB",
                    "INSERT INTO " + TEST_SCHEMA + ".no_pk_table " +
                            "(session_id, duration) VALUES " +
                            "('sess_001', 300)," +
                            "('sess_002', NULL)"
            );
        }

        @Override
        public List<String> cleanupSourceSql() {
            return Collections.singletonList(
                    "DROP TABLE IF EXISTS " + TEST_SCHEMA + ".no_pk_table"
            );
        }
    }

    private static final class UniqueConstraintAsKeyCase implements DialectCase {
        private final ImportCase importCase;

        UniqueConstraintAsKeyCase() {
            SourceTableRef ref = new SourceTableRef(
                    TEST_SCHEMA,
                    "unique_key_table",
                    Collections.emptyList(),
                    null,
                    "default"
            );

            ExpectedYdbTable expectedTable = new ExpectedYdbTable(
                    "mariadb.import_test.unique_key_table",
                    Arrays.asList(
                            new ExpectedYdbColumn("entry_id", PrimitiveType.Int64, false),
                            new ExpectedYdbColumn("category", PrimitiveType.Text, false),
                            new ExpectedYdbColumn("counter_value", PrimitiveType.Int64, true)
                    ),
                    Collections.singletonList("entry_id"),
                    Arrays.asList(
                            ExpectedRow.of(
                                    "entry_id", PrimitiveValue.newInt64(1),
                                    "category", PrimitiveValue.newText("A"),
                                    "counter_value", PrimitiveValue.newInt64(100)
                            ),
                            ExpectedRow.of(
                                    "entry_id", PrimitiveValue.newInt64(2),
                                    "category", PrimitiveValue.newText("B"),
                                    "counter_value", PrimitiveValue.newInt64(200)
                            )
                    )
            );

            this.importCase = new ImportCase(
                    "unique_constraint_as_key",
                    "Table with UNIQUE but no PK (should use UNIQUE as YDB PK)",
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
                    "CREATE TABLE " + TEST_SCHEMA + ".unique_key_table (" +
                            "entry_id BIGINT NOT NULL, " +
                            "category VARCHAR(20) NOT NULL, " +
                            "counter_value BIGINT NULL, " +
                            "UNIQUE (entry_id)" +
                            ") ENGINE=InnoDB",
                    "INSERT INTO " + TEST_SCHEMA + ".unique_key_table " +
                            "(entry_id, category, counter_value) VALUES " +
                            "(1, 'A', 100)," +
                            "(2, 'B', 200)"
            );
        }

        @Override
        public List<String> cleanupSourceSql() {
            return Collections.singletonList(
                    "DROP TABLE IF EXISTS " + TEST_SCHEMA + ".unique_key_table"
            );
        }
    }

    private static final class DecimalCase implements DialectCase {
        private final ImportCase importCase;

        DecimalCase() {
            SourceTableRef ref = new SourceTableRef(
                    TEST_SCHEMA,
                    "decimal_metrics",
                    Collections.singletonList("region_id"),
                    null,
                    "default"
            );

            ExpectedYdbTable expectedTable = new ExpectedYdbTable(
                    "mariadb.import_test.decimal_metrics",
                    Arrays.asList(
                            new ExpectedYdbColumn("region_id", PrimitiveType.Int32, false),
                            new ExpectedYdbColumn("city_name", PrimitiveType.Text, false),
                            new ExpectedYdbColumn("density", DecimalType.of(8, 3), true)
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
                    "CREATE TABLE " + TEST_SCHEMA + ".decimal_metrics (" +
                            "region_id INT NOT NULL PRIMARY KEY, " +
                            "city_name VARCHAR(100) NOT NULL, " +
                            "density DECIMAL(8,3) NULL" +
                            ") ENGINE=InnoDB",
                    "INSERT INTO " + TEST_SCHEMA + ".decimal_metrics " +
                            "(region_id, city_name, density) VALUES " +
                            "(1, 'Moscow', 4.942)," +
                            "(2, 'Novosibirsk', 0.000)"
            );
        }

        @Override
        public List<String> cleanupSourceSql() {
            return Collections.singletonList(
                    "DROP TABLE IF EXISTS " + TEST_SCHEMA + ".decimal_metrics"
            );
        }
    }
}
