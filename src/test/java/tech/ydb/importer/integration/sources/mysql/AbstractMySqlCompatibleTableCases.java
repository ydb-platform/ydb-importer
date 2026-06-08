package tech.ydb.importer.integration.sources.mysql;

import org.junit.jupiter.api.Test;

import tech.ydb.importer.integration.tabletest.AbstractYdbImporterTableTest;

/**
 * Shared table-level tests for MySQL and MariaDB (InnoDB and ColumnStore).
 */
public abstract class AbstractMySqlCompatibleTableCases
        extends AbstractYdbImporterTableTest {


    protected String engine() {
        return "InnoDB";
    }

    @Override
    protected String smallBlob(byte[] data) {
        return "UNHEX('" + hex(data) + "')";
    }

    @Override
    protected String bigBlob(String hexPair, int count) {
        return "UNHEX(REPEAT('" + hexPair + "', " + count + "))";
    }

    /**
     * Covers PK selection priority: explicit PK, PK over UNIQUE,
     * UNIQUE preference by column count then by name.
     */
    @Test
    public void primaryKeySelection() throws Exception {
        String s = schemaName();
        String e = engine();
        importTogether()
                .add(tableTest(s, "pk_composite")
                        .setupSql(
                            "CREATE TABLE " + s + ".pk_composite ("
                            + "region_id INT NOT NULL,"
                            + "city_id INT NOT NULL,"
                            + "name VARCHAR(100) NOT NULL,"
                            + "PRIMARY KEY (region_id, city_id)"
                            + ") ENGINE=" + e + ";"
                            + "INSERT INTO " + s + ".pk_composite VALUES"
                            + " (1,10,'Moscow'),(1,20,'SPb'),"
                            + "(2,30,'Berlin')")
                        .cleanupSql("DROP TABLE IF EXISTS "
                                + s + ".pk_composite")
                        .expectPrimaryKey("region_id", "city_id")
                        .expectRowCount(3)
                        .expectRowExists(
                                "region_id", 1, "city_id", 10,
                                "name", "Moscow"))
                .add(tableTest(s, "pk_over_unique")
                        .setupSql(
                            "CREATE TABLE " + s + ".pk_over_unique ("
                            + "id BIGINT NOT NULL PRIMARY KEY,"
                            + "code VARCHAR(20) NOT NULL,"
                            + "val INT,"
                            + "UNIQUE (code)"
                            + ") ENGINE=" + e + ";"
                            + "INSERT INTO " + s + ".pk_over_unique"
                            + " VALUES (1,'A',10),(2,'B',20)")
                        .cleanupSql("DROP TABLE IF EXISTS "
                                + s + ".pk_over_unique")
                        .expectPrimaryKey("id")
                        .expectRowCount(2)
                        .expectRowExists("id", 1L, "code", "A"))
                .add(tableTest(s, "uq_as_key")
                        .setupSql(
                            "CREATE TABLE " + s + ".uq_as_key ("
                            + "a BIGINT NOT NULL,"
                            + "b VARCHAR(10) NOT NULL,"
                            + "UNIQUE (a)"
                            + ") ENGINE=" + e + ";"
                            + "INSERT INTO " + s + ".uq_as_key"
                            + " VALUES (1,'x'),(2,'y')")
                        .cleanupSql("DROP TABLE IF EXISTS "
                                + s + ".uq_as_key")
                        .expectPrimaryKey("a")
                        .expectRowCount(2))
                .run();
    }

    @Test
    public void emptyTableCreatesSchema() throws Exception {
        String s = schemaName();
        tableTest(s, "empty_tbl")
                .setupSql(
                    "CREATE TABLE " + s + ".empty_tbl ("
                    + "id INT NOT NULL PRIMARY KEY,"
                    + "name VARCHAR(100) NOT NULL"
                    + ") ENGINE=" + engine())
                .cleanupSql("DROP TABLE IF EXISTS "
                        + s + ".empty_tbl")
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
                    + "id INT NOT NULL PRIMARY KEY,"
                    + "val VARCHAR(50) NOT NULL"
                    + ") ENGINE=" + engine() + ";"
                    + "INSERT INTO " + s + ".query_src VALUES"
                    + " (1,'a'),(2,'b'),(3,'c'),(4,'d')")
                .cleanupSql("DROP TABLE IF EXISTS "
                        + s + ".query_src")
                .queryText("SELECT id, val FROM " + s
                        + ".query_src WHERE id <= 2")
                .expectSyntheticKey()
                .expectRowCount(2)
                .expectRowExists("id", 1, "val", "a")
                .expectRowExists("id", 2, "val", "b")
                .run();
    }

    @Test
    public void syntheticKeyDistinguishesRows() throws Exception {
        String s = schemaName();
        tableTest(s, "synth_tbl")
                .setupSql(
                    "CREATE TABLE " + s + ".synth_tbl ("
                    + "seq INT NOT NULL,"
                    + "name VARCHAR(50) NOT NULL"
                    + ") ENGINE=" + engine() + ";"
                    + "INSERT INTO " + s + ".synth_tbl VALUES"
                    + " (1,'same'),(2,'same'),(3,'same')")
                .cleanupSql("DROP TABLE IF EXISTS "
                        + s + ".synth_tbl")
                .expectSyntheticKey()
                .expectRowCount(3)
                .expectRowExists("seq", 1, "name", "same")
                .expectRowExists("seq", 2, "name", "same")
                .expectRowExists("seq", 3, "name", "same")
                .run();
    }

    @Test
    public void partitionedImport() throws Exception {
        String s = schemaName();
        String e = engine();
        tableTest(s, "part_sales")
                .setupSql(
                    "CREATE TABLE " + s + ".part_sales ("
                    + "sale_id INT NOT NULL,"
                    + "region VARCHAR(10) NOT NULL,"
                    + "amount INT NOT NULL,"
                    + "PRIMARY KEY (sale_id, region)"
                    + ") ENGINE=" + e
                    + " PARTITION BY LIST COLUMNS(region) ("
                    + "  PARTITION p_eu VALUES IN ('EU'),"
                    + "  PARTITION p_us VALUES IN ('US')"
                    + ");"
                    + "INSERT INTO " + s + ".part_sales VALUES"
                    + " (1,'EU',100),(2,'EU',200),"
                    + " (3,'US',300),(4,'US',400)")
                .cleanupSql("DROP TABLE IF EXISTS "
                        + s + ".part_sales")
                .expectPrimaryKey("sale_id", "region")
                .expectRowCount(4)
                .expectRowExists("sale_id", 1, "region", "EU", "amount", 100)
                .expectRowExists("sale_id", 4, "region", "US", "amount", 400)
                .run();
    }

    @Test
    public void partitionedMultiBlobSynthKey() throws Exception {
        String s = schemaName();
        String e = engine();

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
                    + "  thumbnail MEDIUMBLOB,"
                    + "  fullsize  MEDIUMBLOB"
                    + ") ENGINE=" + e
                    + " PARTITION BY LIST COLUMNS(region) ("
                    + "  PARTITION p_eu VALUES IN ('EU'),"
                    + "  PARTITION p_us VALUES IN ('US')"
                    + ");"
                    + "INSERT INTO " + s + ".mega_blob VALUES"
                    + "('EU','eu-book-1'," + smallBlob(thumbEU1)
                    + "," + bigBlob("11", 500_000) + "),"
                    + "('EU','eu-book-2'," + smallBlob(thumbEU2)
                    + "," + bigBlob("22", 500_000) + "),"
                    + "('EU','eu-null',NULL,NULL),"
                    + "('US','us-book-1'," + smallBlob(thumbUS1)
                    + "," + bigBlob("33", 500_000) + "),"
                    + "('US','us-book-2'," + smallBlob(thumbUS2)
                    + "," + bigBlob("44", 500_000) + ")")
                .cleanupSql("DROP TABLE IF EXISTS "
                        + s + ".mega_blob")
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
                    + "  id   INT NOT NULL PRIMARY KEY,"
                    + "  name VARCHAR(50) NOT NULL,"
                    + "  data MEDIUMBLOB"
                    + ") ENGINE=" + e + ";"
                    + "INSERT INTO " + s + ".small_blob VALUES"
                    + "  (1, 'hello',     " + smallBlob("Hello".getBytes()) + "),"
                    + "  (2, 'null_blob', NULL),"
                    + "  (3, 'world',     " + smallBlob("World".getBytes()) + ")")
                .cleanupSql("DROP TABLE IF EXISTS "
                        + s + ".small_blob")
                .expectPrimaryKey("id")
                .expectRowCount(3)
                .expectBlobColumn("data")
                .expectBlobBytes("data", "id", 1, "Hello".getBytes())
                .expectBlobBytes("data", "id", 3, "World".getBytes())
                .expectBlobBytes("data", "id", 2, null)
                .run();
    }

    @Test
    public void rangeSplitsByColumnType() throws Exception {
        String s = schemaName();
        String e = engine();
        importTogether()
                .add(tableTest(s, "split_int")
                        .setupSql(
                            "CREATE TABLE " + s + ".split_int ("
                            + "  id  INT PRIMARY KEY,"
                            + "  val INT NOT NULL"
                            + ") ENGINE=" + e + ";"
                            + "INSERT INTO " + s + ".split_int VALUES"
                            + "  (1,5),(2,12),(3,28),(4,45),"
                            + "  (5,60),(6,73),(7,88),(8,99)")
                        .cleanupSql("DROP TABLE IF EXISTS " + s + ".split_int")
                        .splitBy("val").splitFrom("0").splitTo("100").splitCount(4)
                        .expectPrimaryKey("id")
                        .expectRowCount(8))
                .add(tableTest(s, "split_dec")
                        .setupSql(
                            "CREATE TABLE " + s + ".split_dec ("
                            + "  id  INT PRIMARY KEY,"
                            + "  val DECIMAL(10,2) NOT NULL"
                            + ") ENGINE=" + e + ";"
                            + "INSERT INTO " + s + ".split_dec VALUES"
                            + "  (1,1.50),(2,10.25),(3,33.75),"
                            + "  (4,50.00),(5,75.50),(6,99.99)")
                        .cleanupSql("DROP TABLE IF EXISTS " + s + ".split_dec")
                        .splitBy("val").splitFrom("0").splitTo("100").splitCount(3)
                        .expectPrimaryKey("id")
                        .expectRowCount(6))
                .add(tableTest(s, "split_dbl")
                        .setupSql(
                            "CREATE TABLE " + s + ".split_dbl ("
                            + "  id  INT PRIMARY KEY,"
                            + "  val DOUBLE NOT NULL"
                            + ") ENGINE=" + e + ";"
                            + "INSERT INTO " + s + ".split_dbl VALUES"
                            + "  (1,0.1),(2,1.5),(3,3.7),(4,5.0),(5,7.2)")
                        .cleanupSql("DROP TABLE IF EXISTS " + s + ".split_dbl")
                        .splitBy("val").splitFrom("0").splitTo("10").splitCount(3)
                        .expectPrimaryKey("id")
                        .expectRowCount(5))
                .add(tableTest(s, "split_date")
                        .setupSql(
                            "CREATE TABLE " + s + ".split_date ("
                            + "  id  INT PRIMARY KEY,"
                            + "  val DATE NOT NULL"
                            + ") ENGINE=" + e + ";"
                            + "INSERT INTO " + s + ".split_date VALUES"
                            + "  (1,'2024-01-15'),(2,'2024-03-10'),"
                            + "  (3,'2024-06-01'),(4,'2024-09-20'),"
                            + "  (5,'2024-12-25')")
                        .cleanupSql("DROP TABLE IF EXISTS " + s + ".split_date")
                        .splitBy("val")
                            .splitFrom("2024-01-01").splitTo("2025-01-01")
                            .splitCount(4)
                        .expectPrimaryKey("id")
                        .expectRowCount(5))
                .add(tableTest(s, "split_ts")
                        .setupSql(
                            "CREATE TABLE " + s + ".split_ts ("
                            + "  id  INT PRIMARY KEY,"
                            + "  val DATETIME NOT NULL"
                            + ") ENGINE=" + e + ";"
                            + "INSERT INTO " + s + ".split_ts VALUES"
                            + "  (1,'2024-01-15 10:00:00'),"
                            + "  (2,'2024-06-01 14:30:00'),"
                            + "  (3,'2024-12-25 23:59:59')")
                        .cleanupSql("DROP TABLE IF EXISTS " + s + ".split_ts")
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
                    + "  id    INT PRIMARY KEY,"
                    + "  short TEXT,"
                    + "  large LONGTEXT"
                    + ") ENGINE=" + engine())
                .insertRow("INSERT INTO " + s + ".clob_docs VALUES (?, ?, ?)",
                        1, "YDB", bigText)
                .insertRow("INSERT INTO " + s + ".clob_docs VALUES (?, ?, ?)",
                        2, null, null)
                .insertRow("INSERT INTO " + s + ".clob_docs VALUES (?, ?, ?)",
                        3, "", "")
                .cleanupSql("DROP TABLE IF EXISTS " + s + ".clob_docs")
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
