package tech.ydb.importer.integration.sources.mysql;

import org.junit.jupiter.api.Test;

import tech.ydb.importer.config.TableRef;
import tech.ydb.importer.integration.tabletest.AbstractYdbImporterTableTest;

public abstract class AbstractMySqlPartitioningTests
        extends AbstractYdbImporterTableTest {

    protected String engine() {
        return "InnoDB";
    }

    private String seriesInsert(String table, int n) {
        return "INSERT INTO " + table + " (id, val) "
                + "WITH RECURSIVE seq(n) AS ("
                + "  SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n < " + n
                + ") SELECT n, n FROM seq";
    }

    @Test
    public void mirrorSuccessful() throws Exception {
        String s = schemaName();
        tableTest(s, "ap_mirror_ok")
                .setupSql(
                    "CREATE TABLE " + s + ".ap_mirror_ok ("
                    + "  id  INT NOT NULL,"
                    + "  val INT NOT NULL,"
                    + "  PRIMARY KEY (id)"
                    + ") ENGINE=" + engine()
                    + " PARTITION BY RANGE (id) ("
                    + "  PARTITION p1 VALUES LESS THAN (100),"
                    + "  PARTITION p2 VALUES LESS THAN (200),"
                    + "  PARTITION p3 VALUES LESS THAN MAXVALUE);"
                    + "INSERT INTO " + s + ".ap_mirror_ok VALUES"
                    + "  (1, 10), (50, 20), (120, 30), (210, 40)")
                .cleanupSql("DROP TABLE IF EXISTS " + s + ".ap_mirror_ok")
                .ydbPartitionCount(TableRef.AUTO)
                .expectPrimaryKey("id")
                .expectRowCount(4)
                .expectPartitionCount(3)
                .run();
    }

    @Test
    public void mirrorSkippedOnOverlap() throws Exception {
        String s = schemaName();
        tableTest(s, "ap_mirror_overlap")
                .setupSql(
                    "CREATE TABLE " + s + ".ap_mirror_overlap ("
                    + "  id     INT NOT NULL,"
                    + "  region INT NOT NULL,"
                    + "  PRIMARY KEY (id, region)"
                    + ") ENGINE=" + engine()
                    + " PARTITION BY LIST (region) ("
                    + "  PARTITION p_a VALUES IN (1),"
                    + "  PARTITION p_b VALUES IN (2));"
                    + "INSERT INTO " + s + ".ap_mirror_overlap VALUES"
                    + "  (1, 1), (2, 2), (3, 1), (4, 2)")
                .cleanupSql("DROP TABLE IF EXISTS " + s + ".ap_mirror_overlap")
                .ydbPartitionCount(TableRef.AUTO)
                .expectPrimaryKey("id", "region")
                .expectRowCount(4)
                .expectPartitionCount(1)
                .run();
    }

    @Test
    public void mirrorSkippedOnNonPartitioned() throws Exception {
        String s = schemaName();
        tableTest(s, "ap_no_partitions")
                .setupSql(
                    "CREATE TABLE " + s + ".ap_no_partitions ("
                    + "  id  INT PRIMARY KEY,"
                    + "  val INT"
                    + ") ENGINE=" + engine() + ";"
                    + "INSERT INTO " + s + ".ap_no_partitions VALUES"
                    + "  (1, 10), (2, 20), (3, 30)")
                .cleanupSql("DROP TABLE IF EXISTS " + s + ".ap_no_partitions")
                .ydbPartitionCount(TableRef.AUTO)
                .expectPrimaryKey("id")
                .expectRowCount(3)
                .expectPartitionCount(1)
                .run();
    }

    @Test
    public void explicitNGlobalMinMax() throws Exception {
        String s = schemaName();
        tableTest(s, "ap_explicit_n")
                .setupSql(
                    "CREATE TABLE " + s + ".ap_explicit_n ("
                    + "  id  INT PRIMARY KEY,"
                    + "  val INT"
                    + ") ENGINE=" + engine() + ";"
                    + seriesInsert(s + ".ap_explicit_n", 100))
                .cleanupSql("DROP TABLE IF EXISTS " + s + ".ap_explicit_n")
                .ydbPartitionCount(8)
                .expectPrimaryKey("id")
                .expectRowCount(100)
                .expectPartitionCount(8)
                .run();
    }

    @Test
    public void explicitNManualBounds() throws Exception {
        String s = schemaName();
        tableTest(s, "ap_manual_bounds")
                .setupSql(
                    "CREATE TABLE " + s + ".ap_manual_bounds ("
                    + "  id  INT PRIMARY KEY,"
                    + "  val INT"
                    + ") ENGINE=" + engine() + ";"
                    + seriesInsert(s + ".ap_manual_bounds", 50))
                .cleanupSql("DROP TABLE IF EXISTS " + s + ".ap_manual_bounds")
                .ydbPartitionCount(5)
                .ydbPartitionFrom("1")
                .ydbPartitionTo("100")
                .expectPrimaryKey("id")
                .expectRowCount(50)
                .expectPartitionCount(5)
                .run();
    }

    @Test
    public void noneLiteralDisablesPreSplit() throws Exception {
        String s = schemaName();
        tableTest(s, "ap_none")
                .setupSql(
                    "CREATE TABLE " + s + ".ap_none ("
                    + "  id  INT PRIMARY KEY,"
                    + "  val INT"
                    + ") ENGINE=" + engine() + ";"
                    + "INSERT INTO " + s + ".ap_none VALUES (1, 10), (2, 20)")
                .cleanupSql("DROP TABLE IF EXISTS " + s + ".ap_none")
                .ydbPartitionCount(TableRef.NONE)
                .expectPrimaryKey("id")
                .expectRowCount(2)
                .expectPartitionCount(1)
                .run();
    }

    @Test
    public void explicitNIgnoresSourcePartitions() throws Exception {
        String s = schemaName();
        tableTest(s, "ap_explicit_ignore")
                .setupSql(
                    "CREATE TABLE " + s + ".ap_explicit_ignore ("
                    + "  id  INT NOT NULL,"
                    + "  val INT NOT NULL,"
                    + "  PRIMARY KEY (id)"
                    + ") ENGINE=" + engine()
                    + " PARTITION BY RANGE (id) ("
                    + "  PARTITION p1 VALUES LESS THAN (100),"
                    + "  PARTITION p2 VALUES LESS THAN MAXVALUE);"
                    + seriesInsert(s + ".ap_explicit_ignore", 100))
                .cleanupSql("DROP TABLE IF EXISTS " + s + ".ap_explicit_ignore")
                .ydbPartitionCount(8)
                .expectPrimaryKey("id")
                .expectRowCount(100)
                .expectPartitionCount(8)
                .run();
    }

    @Test
    public void splitCountAutoInheritsMirror() throws Exception {
        String s = schemaName();
        tableTest(s, "ap_split_inherit")
                .setupSql(
                    "CREATE TABLE " + s + ".ap_split_inherit ("
                    + "  id  INT NOT NULL,"
                    + "  val INT NOT NULL,"
                    + "  PRIMARY KEY (id)"
                    + ") ENGINE=" + engine()
                    + " PARTITION BY RANGE (id) ("
                    + "  PARTITION p1 VALUES LESS THAN (100),"
                    + "  PARTITION p2 VALUES LESS THAN (200),"
                    + "  PARTITION p3 VALUES LESS THAN MAXVALUE);"
                    + seriesInsert(s + ".ap_split_inherit", 300))
                .cleanupSql("DROP TABLE IF EXISTS " + s + ".ap_split_inherit")
                .ydbPartitionCount(TableRef.AUTO)
                .splitBy("id")
                .splitCount(TableRef.AUTO)
                .useSourcePartitions(false)
                .expectPrimaryKey("id")
                .expectRowCount(300)
                .expectPartitionCount(3)
                .run();
    }

    @Test
    public void autoWithExplicitSplitCount() throws Exception {
        String s = schemaName();
        tableTest(s, "ap_auto_split_count")
                .setupSql(
                    "CREATE TABLE " + s + ".ap_auto_split_count ("
                    + "  id  INT NOT NULL,"
                    + "  val INT NOT NULL,"
                    + "  PRIMARY KEY (id)"
                    + ") ENGINE=" + engine()
                    + " PARTITION BY RANGE (id) ("
                    + "  PARTITION p1 VALUES LESS THAN (100),"
                    + "  PARTITION p2 VALUES LESS THAN (200),"
                    + "  PARTITION p3 VALUES LESS THAN MAXVALUE);"
                    + seriesInsert(s + ".ap_auto_split_count", 300))
                .cleanupSql("DROP TABLE IF EXISTS " + s + ".ap_auto_split_count")
                .ydbPartitionCount(TableRef.AUTO)
                .splitBy("id")
                .splitCount(6)
                .expectPrimaryKey("id")
                .expectRowCount(300)
                .expectPartitionCount(6)
                .run();
    }

    @Test
    public void splitByAutoResolvesLeadingPk() throws Exception {
        String s = schemaName();
        tableTest(s, "ap_split_by_auto")
                .setupSql(
                    "CREATE TABLE " + s + ".ap_split_by_auto ("
                    + "  id  INT PRIMARY KEY,"
                    + "  val INT"
                    + ") ENGINE=" + engine() + ";"
                    + seriesInsert(s + ".ap_split_by_auto", 40))
                .cleanupSql("DROP TABLE IF EXISTS " + s + ".ap_split_by_auto")
                .ydbPartitionCount(TableRef.NONE)
                .splitBy(TableRef.SPLIT_BY_AUTO)
                .splitCount(4)
                .expectPrimaryKey("id")
                .expectRowCount(40)
                .expectPartitionCount(1)
                .run();
    }

    @Test
    public void splitCountAutoNoResolvable() throws Exception {
        String s = schemaName();
        tableTest(s, "ap_split_no_inherit")
                .setupSql(
                    "CREATE TABLE " + s + ".ap_split_no_inherit ("
                    + "  id  INT PRIMARY KEY,"
                    + "  val INT"
                    + ") ENGINE=" + engine() + ";"
                    + "INSERT INTO " + s + ".ap_split_no_inherit VALUES"
                    + "  (1, 10), (2, 20), (3, 30)")
                .cleanupSql("DROP TABLE IF EXISTS " + s + ".ap_split_no_inherit")
                .ydbPartitionCount(TableRef.AUTO)
                .splitBy("id")
                .splitCount(TableRef.AUTO)
                .expectPrimaryKey("id")
                .expectRowCount(3)
                .expectPartitionCount(1)
                .run();
    }
}
