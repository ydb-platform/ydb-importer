package tech.ydb.importer.integration.sources.postgres;

import org.junit.jupiter.api.Test;

import tech.ydb.importer.integration.tabletest.AbstractYdbImporterTableTest;

/**
 * Shared table-level tests for PostgreSQL-compatible databases
 * (PostgreSQL, Greenplum).
 */
public abstract class AbstractPostgresCompatibleTableCases
        extends AbstractYdbImporterTableTest {

    @Override
    protected String smallBlob(byte[] data) {
        return "lo_from_bytea(0, decode('" + hex(data) + "', 'hex'))";
    }

    @Override
    protected String bigBlob(String hexPair, int count) {
        return "lo_from_bytea(0, decode(repeat('" + hexPair + "', "
                + count + "), 'hex'))";
    }

    /**
     * Covers PK selection priority: explicit PK, PK over UNIQUE,
     * UNIQUE preference by column count then by name.
     */
    @Test
    public void primaryKeySelection() throws Exception {
        String s = schemaName();
        importTogether()
                .add(tableTest(s, "pk_composite")
                        .setupSql(
                            "CREATE TABLE " + s + ".pk_composite ("
                            + "  region_id INTEGER NOT NULL,"
                            + "  city_id   INTEGER NOT NULL,"
                            + "  name      VARCHAR(100) NOT NULL,"
                            + "  PRIMARY KEY (region_id, city_id)"
                            + ");"
                            + "INSERT INTO " + s + ".pk_composite VALUES"
                            + "  (1, 10, 'Moscow'),"
                            + "  (1, 20, 'SPb'),"
                            + "  (2, 30, 'Berlin')")
                        .cleanupSql(
                            "DROP TABLE IF EXISTS "
                            + s + ".pk_composite CASCADE")
                        .expectPrimaryKey("region_id", "city_id")
                        .expectRowCount(3)
                        .expectRowExists(
                                "region_id", 1, "city_id", 10,
                                "name", "Moscow"))
                .add(tableTest(s, "pk_over_unique")
                        .setupSql(
                            "CREATE TABLE " + s + ".pk_over_unique ("
                            + "  id   BIGINT PRIMARY KEY,"
                            + "  code VARCHAR(20) NOT NULL UNIQUE,"
                            + "  val  INTEGER"
                            + ");"
                            + "INSERT INTO " + s + ".pk_over_unique VALUES"
                            + "  (1, 'A', 10),"
                            + "  (2, 'B', 20)")
                        .cleanupSql(
                            "DROP TABLE IF EXISTS "
                            + s + ".pk_over_unique CASCADE")
                        .expectPrimaryKey("id")
                        .expectRowCount(2)
                        .expectRowExists("id", 1L, "code", "A"))
                .add(tableTest(s, "uq_fewer_cols")
                        .setupSql(
                            "CREATE TABLE " + s + ".uq_fewer_cols ("
                            + "  a BIGINT NOT NULL,"
                            + "  b VARCHAR(10) NOT NULL,"
                            + "  c VARCHAR(10) NOT NULL UNIQUE,"
                            + "  UNIQUE (a, b)"
                            + ");"
                            + "INSERT INTO " + s + ".uq_fewer_cols VALUES"
                            + "  (1, 'x', 'p'),"
                            + "  (2, 'y', 'q')")
                        .cleanupSql(
                            "DROP TABLE IF EXISTS "
                            + s + ".uq_fewer_cols CASCADE")
                        .expectPrimaryKey("c")
                        .expectRowCount(2)
                        .expectRowExists("c", "p"))
                .add(tableTest(s, "uq_sorted_names")
                        .setupSql(
                            "CREATE TABLE " + s + ".uq_sorted_names ("
                            + "  x BIGINT NOT NULL,"
                            + "  a VARCHAR(10) NOT NULL,"
                            + "  b VARCHAR(10) NOT NULL,"
                            + "  z VARCHAR(10) NOT NULL,"
                            + "  UNIQUE (z, a),"
                            + "  UNIQUE (a, b)"
                            + ");"
                            + "INSERT INTO " + s + ".uq_sorted_names VALUES"
                            + "  (1, 'v1', 'v2', 'v3'),"
                            + "  (2, 'v4', 'v5', 'v6')")
                        .cleanupSql(
                            "DROP TABLE IF EXISTS "
                            + s + ".uq_sorted_names CASCADE")
                        .expectPrimaryKey("a", "b")
                        .expectRowCount(2)
                        .expectRowExists("a", "v1", "b", "v2"))
                .add(tableTest(s, "uq_expression")
                        .setupSql(
                            "CREATE TABLE " + s + ".uq_expression ("
                            + "  id    BIGINT NOT NULL,"
                            + "  email VARCHAR(100) NOT NULL"
                            + ");"
                            + "CREATE UNIQUE INDEX ON " + s + ".uq_expression"
                            + "  (lower(email));"
                            + "INSERT INTO " + s + ".uq_expression VALUES"
                            + "  (1, 'A@x'),"
                            + "  (2, 'B@x')")
                        .cleanupSql(
                            "DROP TABLE IF EXISTS "
                            + s + ".uq_expression CASCADE")
                        .expectSyntheticKey()
                        .expectRowCount(2))
                .add(tableTest(s, "uq_partial")
                        .setupSql(
                            "CREATE TABLE " + s + ".uq_partial ("
                            + "  id         BIGINT NOT NULL,"
                            + "  email      VARCHAR(100) NOT NULL,"
                            + "  deleted_at TIMESTAMP"
                            + ");"
                            + "CREATE UNIQUE INDEX ON " + s + ".uq_partial"
                            + "  (email) WHERE deleted_at IS NULL;"
                            + "INSERT INTO " + s + ".uq_partial VALUES"
                            + "  (1, 'x@y', NULL),"
                            + "  (2, 'x@y', TIMESTAMP '2024-01-01'),"
                            + "  (3, 'z@y', NULL)")
                        .cleanupSql(
                            "DROP TABLE IF EXISTS "
                            + s + ".uq_partial CASCADE")
                        .expectSyntheticKey()
                        .expectRowCount(3))
                .run();
    }

    @Test
    public void partitionedImport() throws Exception {
        String s = schemaName();
        tableTest(s, "part_sales")
                .setupSql(
                    "CREATE TABLE " + s + ".part_sales ("
                    + "  id     INTEGER NOT NULL,"
                    + "  region INTEGER NOT NULL,"
                    + "  amount INTEGER NOT NULL,"
                    + "  PRIMARY KEY (id, region)"
                    + ") PARTITION BY RANGE (region);"
                    + "CREATE TABLE " + s + ".part_sales_r1"
                    + "  PARTITION OF " + s + ".part_sales"
                    + "  FOR VALUES FROM (1) TO (100);"
                    + "CREATE TABLE " + s + ".part_sales_r2"
                    + "  PARTITION OF " + s + ".part_sales"
                    + "  FOR VALUES FROM (100) TO (200);"
                    + "CREATE TABLE " + s + ".part_sales_r3"
                    + "  PARTITION OF " + s + ".part_sales"
                    + "  FOR VALUES FROM (200) TO (300);"
                    + "INSERT INTO " + s + ".part_sales VALUES"
                    + "  (1,  10, 100),"
                    + "  (2,  50, 200),"
                    + "  (3, 110, 300),"
                    + "  (4, 150, 400),"
                    + "  (5, 210, 500)")
                .cleanupSql(
                    "DROP TABLE IF EXISTS "
                    + s + ".part_sales CASCADE")
                .expectPrimaryKey("id", "region")
                .expectRowCount(5)
                .expectRowExists("id", 1, "amount", 100)
                .expectRowExists("id", 5, "amount", 500)
                .run();
    }

    @Test
    public void emptyTableCreatesSchema() throws Exception {
        String s = schemaName();
        tableTest(s, "empty_tbl")
                .setupSql(
                    "CREATE TABLE " + s + ".empty_tbl ("
                    + "  id   INTEGER PRIMARY KEY,"
                    + "  name VARCHAR(100) NOT NULL"
                    + ")")
                .cleanupSql(
                    "DROP TABLE IF EXISTS "
                    + s + ".empty_tbl CASCADE")
                .expectPrimaryKey("id")
                .expectRowCount(0)
                .run();
    }

    @Test
    public void customQueryTextImport() throws Exception {
        String s = schemaName();
        tableTest(s, "query_src")
                .setupSql(
                    "CREATE TABLE " + s + ".query_src ("
                    + "  id  INTEGER PRIMARY KEY,"
                    + "  val VARCHAR(50) NOT NULL"
                    + ");"
                    + "INSERT INTO " + s + ".query_src VALUES"
                    + "  (1,'a'),(2,'b'),(3,'c'),(4,'d')")
                .cleanupSql(
                    "DROP TABLE IF EXISTS "
                    + s + ".query_src CASCADE")
                .queryText("SELECT id, val FROM " + s + ".query_src"
                        + " WHERE id <= 2")
                .expectSyntheticKey()
                .expectRowCount(2)
                .expectRowExists("id", 1, "val", "a")
                .expectRowExists("id", 2, "val", "b")
                .run();
    }

    @Test
    public void skipUnsupportedColumns() throws Exception {
        String s = schemaName();
        tableTest(s, "skip_pg")
                .withOptions(opts -> opts.setSkipUnknownTypes(true))
                .setupSql(
                    "CREATE TABLE " + s + ".skip_pg ("
                    + "  id   INTEGER PRIMARY KEY,"
                    + "  name VARCHAR(50) NOT NULL,"
                    + "  uid  UUID,"
                    + "  doc  JSONB,"
                    + "  tags INTEGER[]"
                    + ");"
                    + "INSERT INTO " + s + ".skip_pg"
                    + "  (id, name, uid, doc, tags) VALUES"
                    + "  (1, 'a', gen_random_uuid(),"
                    + "   '{\"k\":1}', ARRAY[1,2]),"
                    + "  (2, 'b', gen_random_uuid(),"
                    + "   '{\"k\":2}', ARRAY[3,4])")
                .cleanupSql(
                    "DROP TABLE IF EXISTS "
                    + s + ".skip_pg CASCADE")
                .expectPrimaryKey("id")
                .expectSkippedColumns("uid", "doc", "tags")
                .expectRowCount(2)
                .expectRowExists("id", 1, "name", "a")
                .expectRowExists("id", 2, "name", "b")
                .run();
    }

    @Test
    public void partitionedMultiBlobSynthKey() throws Exception {
        String s = schemaName();

        byte[] thumbEU1 = "thumb-eu-1".getBytes();
        byte[] thumbEU2 = "thumb-eu-2".getBytes();
        byte[] thumbUS1 = "thumb-us-1".getBytes();
        byte[] thumbUS2 = "thumb-us-2".getBytes();

        byte[] fullEU1 = filled(500_000, (byte) 0x11);
        byte[] fullEU2 = filled(500_000, (byte) 0x22);
        byte[] fullUS1 = filled(500_000, (byte) 0x33);
        byte[] fullUS2 = filled(500_000, (byte) 0x44);

        tableTest(s, "mega_blob")
                .setupSql(
                    "CREATE TABLE " + s + ".mega_blob ("
                    + "  region    VARCHAR(10) NOT NULL,"
                    + "  label     VARCHAR(30),"
                    + "  thumbnail OID,"
                    + "  fullsize  OID"
                    + ") PARTITION BY LIST (region);"
                    + "CREATE TABLE " + s + ".mega_blob_eu PARTITION OF"
                    + " " + s + ".mega_blob FOR VALUES IN ('EU');"
                    + "CREATE TABLE " + s + ".mega_blob_us PARTITION OF"
                    + " " + s + ".mega_blob FOR VALUES IN ('US');"
                    + "INSERT INTO " + s + ".mega_blob VALUES"
                    + "('EU', 'eu-book-1', " + smallBlob(thumbEU1)
                    + ", " + bigBlob("11", 500_000) + "),"
                    + "('EU', 'eu-book-2', " + smallBlob(thumbEU2)
                    + ", " + bigBlob("22", 500_000) + "),"
                    + "('EU', 'eu-null', NULL, NULL),"
                    + "('US', 'us-book-1', " + smallBlob(thumbUS1)
                    + ", " + bigBlob("33", 500_000) + "),"
                    + "('US', 'us-book-2', " + smallBlob(thumbUS2)
                    + ", " + bigBlob("44", 500_000) + ")")
                .cleanupSql(
                    "DROP TABLE IF EXISTS " + s + ".mega_blob CASCADE")
                .expectSyntheticKey()
                .expectRowCount(5)
                .expectBlobColumn("thumbnail")
                .expectBlobColumn("fullsize")
                .expectBlobBytes("thumbnail", "label", "eu-book-1", thumbEU1)
                .expectBlobBytes("thumbnail", "label", "eu-book-2", thumbEU2)
                .expectBlobBytes("thumbnail", "label", "eu-null", null)
                .expectBlobBytes("thumbnail", "label", "us-book-1", thumbUS1)
                .expectBlobBytes("thumbnail", "label", "us-book-2", thumbUS2)
                .expectBlobBytes("fullsize", "label", "eu-book-1", fullEU1)
                .expectBlobBytes("fullsize", "label", "eu-book-2", fullEU2)
                .expectBlobBytes("fullsize", "label", "eu-null", null)
                .expectBlobBytes("fullsize", "label", "us-book-1", fullUS1)
                .expectBlobBytes("fullsize", "label", "us-book-2", fullUS2)
                .run();

        tableTest(s, "small_blob")
                .setupSql(
                    "CREATE TABLE " + s + ".small_blob ("
                    + "  id   INTEGER PRIMARY KEY,"
                    + "  name VARCHAR(50) NOT NULL,"
                    + "  data OID"
                    + ");"
                    + "INSERT INTO " + s + ".small_blob VALUES"
                    + "  (1, 'hello',     " + smallBlob("Hello".getBytes()) + "),"
                    + "  (2, 'null_blob', NULL),"
                    + "  (3, 'world',     " + smallBlob("World".getBytes()) + ")")
                .cleanupSql(
                    "DROP TABLE IF EXISTS " + s + ".small_blob CASCADE")
                .expectPrimaryKey("id")
                .expectRowCount(3)
                .expectBlobColumn("data")
                .expectBlobBytes("data", "id", 1, "Hello".getBytes())
                .expectBlobBytes("data", "id", 3, "World".getBytes())
                .expectBlobBytes("data", "id", 2, null)
                .run();
    }

    @Test
    public void syntheticKeyDistinguishesRows() throws Exception {
        String s = schemaName();
        tableTest(s, "synth_pg")
                .setupSql(
                    "CREATE TABLE " + s + ".synth_pg ("
                    + "  seq  INTEGER NOT NULL,"
                    + "  name VARCHAR(50) NOT NULL"
                    + ");"
                    + "INSERT INTO " + s + ".synth_pg VALUES"
                    + "  (1, 'same'), (2, 'same'), (3, 'same')")
                .cleanupSql(
                    "DROP TABLE IF EXISTS "
                    + s + ".synth_pg CASCADE")
                .expectSyntheticKey()
                .expectRowCount(3)
                .expectRowExists("seq", 1, "name", "same")
                .expectRowExists("seq", 2, "name", "same")
                .expectRowExists("seq", 3, "name", "same")
                .run();
    }

    @Test
    public void rangeSplitsByColumnType() throws Exception {
        String s = schemaName();
        importTogether()
                .add(tableTest(s, "split_int")
                        .setupSql(
                            "CREATE TABLE " + s + ".split_int ("
                            + "  id  INTEGER PRIMARY KEY,"
                            + "  val INTEGER NOT NULL"
                            + ");"
                            + "INSERT INTO " + s + ".split_int VALUES"
                            + "  (1,5),(2,12),(3,28),(4,45),"
                            + "  (5,60),(6,73),(7,88),(8,99)")
                        .cleanupSql(
                            "DROP TABLE IF EXISTS " + s + ".split_int CASCADE")
                        .splitBy("val").splitFrom("0").splitTo("100").splitCount(4)
                        .expectPrimaryKey("id")
                        .expectRowCount(8))
                .add(tableTest(s, "split_dec")
                        .setupSql(
                            "CREATE TABLE " + s + ".split_dec ("
                            + "  id  INTEGER PRIMARY KEY,"
                            + "  val DECIMAL(10,2) NOT NULL"
                            + ");"
                            + "INSERT INTO " + s + ".split_dec VALUES"
                            + "  (1,1.50),(2,10.25),(3,33.75),"
                            + "  (4,50.00),(5,75.50),(6,99.99)")
                        .cleanupSql(
                            "DROP TABLE IF EXISTS " + s + ".split_dec CASCADE")
                        .splitBy("val").splitFrom("0").splitTo("100").splitCount(3)
                        .expectPrimaryKey("id")
                        .expectRowCount(6))
                .add(tableTest(s, "split_dbl")
                        .setupSql(
                            "CREATE TABLE " + s + ".split_dbl ("
                            + "  id  INTEGER PRIMARY KEY,"
                            + "  val DOUBLE PRECISION NOT NULL"
                            + ");"
                            + "INSERT INTO " + s + ".split_dbl VALUES"
                            + "  (1,0.1),(2,1.5),(3,3.7),(4,5.0),(5,7.2)")
                        .cleanupSql(
                            "DROP TABLE IF EXISTS " + s + ".split_dbl CASCADE")
                        .splitBy("val").splitFrom("0").splitTo("10").splitCount(3)
                        .expectPrimaryKey("id")
                        .expectRowCount(5))
                .add(tableTest(s, "split_date")
                        .setupSql(
                            "CREATE TABLE " + s + ".split_date ("
                            + "  id  INTEGER PRIMARY KEY,"
                            + "  val DATE NOT NULL"
                            + ");"
                            + "INSERT INTO " + s + ".split_date VALUES"
                            + "  (1,'2024-01-15'),(2,'2024-03-10'),"
                            + "  (3,'2024-06-01'),(4,'2024-09-20'),"
                            + "  (5,'2024-12-25')")
                        .cleanupSql(
                            "DROP TABLE IF EXISTS " + s + ".split_date CASCADE")
                        .splitBy("val")
                            .splitFrom("2024-01-01").splitTo("2025-01-01")
                            .splitCount(4)
                        .expectPrimaryKey("id")
                        .expectRowCount(5))
                .add(tableTest(s, "split_ts")
                        .setupSql(
                            "CREATE TABLE " + s + ".split_ts ("
                            + "  id  INTEGER PRIMARY KEY,"
                            + "  val TIMESTAMP NOT NULL"
                            + ");"
                            + "INSERT INTO " + s + ".split_ts VALUES"
                            + "  (1,'2024-01-15 10:00:00'),"
                            + "  (2,'2024-06-01 14:30:00'),"
                            + "  (3,'2024-12-25 23:59:59')")
                        .cleanupSql(
                            "DROP TABLE IF EXISTS " + s + ".split_ts CASCADE")
                        .splitBy("val")
                            .splitFrom("2024-01-01 00:00:00")
                            .splitTo("2025-01-01 00:00:00")
                            .splitCount(3)
                        .expectPrimaryKey("id")
                        .expectRowCount(3))
                .run();
    }

    @Test
    public void clobContentImport() throws Exception {
        String s = schemaName();
        String bigText = repeat("Y", 524288);

        tableTest(s, "clob_docs")
                .setupSql(
                    "CREATE TABLE " + s + ".clob_docs ("
                    + "  id    INTEGER PRIMARY KEY,"
                    + "  short TEXT,"
                    + "  large TEXT"
                    + ")")
                .insertRow("INSERT INTO " + s + ".clob_docs VALUES (?, ?, ?)",
                        1, "YDB", bigText)
                .insertRow("INSERT INTO " + s + ".clob_docs VALUES (?, ?, ?)",
                        2, null, null)
                .insertRow("INSERT INTO " + s + ".clob_docs VALUES (?, ?, ?)",
                        3, "", "")
                .cleanupSql("DROP TABLE IF EXISTS " + s + ".clob_docs CASCADE")
                .clobColumns("short", "large")
                .expectPrimaryKey("id")
                .expectRowCount(3)
                .expectClobColumn("short")
                .expectClobColumn("large")
                .expectClobContent("short", "id", 1, "YDB")
                .expectClobContent("large", "id", 1, bigText)
                .expectClobContent("short", "id", 2, null)
                .expectClobContent("large", "id", 2, null)
                .expectClobContent("short", "id", 3, "")
                .expectClobContent("large", "id", 3, "")
                .run();
    }
}
