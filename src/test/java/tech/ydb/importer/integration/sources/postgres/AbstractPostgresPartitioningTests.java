package tech.ydb.importer.integration.sources.postgres;

import org.junit.jupiter.api.Test;

import tech.ydb.importer.config.TableRef;
import tech.ydb.importer.integration.tabletest.AbstractYdbImporterTableTest;

public abstract class AbstractPostgresPartitioningTests
        extends AbstractYdbImporterTableTest {

    @Test
    public void mirrorSuccessful() throws Exception {
        String s = schemaName();
        tableTest(s, "ap_mirror_ok")
                .setupSql(
                    "CREATE TABLE " + s + ".ap_mirror_ok ("
                    + "  id INTEGER NOT NULL,"
                    + "  val INTEGER NOT NULL,"
                    + "  PRIMARY KEY (id)"
                    + ") PARTITION BY RANGE (id);"
                    + "CREATE TABLE " + s + ".ap_mirror_ok_p1"
                    + "  PARTITION OF " + s + ".ap_mirror_ok"
                    + "  FOR VALUES FROM (MINVALUE) TO (100);"
                    + "CREATE TABLE " + s + ".ap_mirror_ok_p2"
                    + "  PARTITION OF " + s + ".ap_mirror_ok"
                    + "  FOR VALUES FROM (100) TO (200);"
                    + "CREATE TABLE " + s + ".ap_mirror_ok_p3"
                    + "  PARTITION OF " + s + ".ap_mirror_ok"
                    + "  FOR VALUES FROM (200) TO (MAXVALUE);"
                    + "INSERT INTO " + s + ".ap_mirror_ok VALUES"
                    + "  (1, 10), (50, 20), (120, 30), (210, 40)")
                .cleanupSql("DROP TABLE IF EXISTS " + s + ".ap_mirror_ok CASCADE")
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
                    + "  id INTEGER NOT NULL,"
                    + "  region INTEGER NOT NULL,"
                    + "  PRIMARY KEY (id, region)"
                    + ") PARTITION BY LIST (region);"
                    + "CREATE TABLE " + s + ".ap_mirror_overlap_a"
                    + "  PARTITION OF " + s + ".ap_mirror_overlap"
                    + "  FOR VALUES IN (1);"
                    + "CREATE TABLE " + s + ".ap_mirror_overlap_b"
                    + "  PARTITION OF " + s + ".ap_mirror_overlap"
                    + "  FOR VALUES IN (2);"
                    + "INSERT INTO " + s + ".ap_mirror_overlap VALUES"
                    + "  (1, 1), (2, 2), (3, 1), (4, 2)")
                .cleanupSql("DROP TABLE IF EXISTS " + s + ".ap_mirror_overlap CASCADE")
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
                    + "  id INTEGER PRIMARY KEY,"
                    + "  val INTEGER"
                    + ");"
                    + "INSERT INTO " + s + ".ap_no_partitions VALUES"
                    + "  (1, 10), (2, 20), (3, 30)")
                .cleanupSql("DROP TABLE IF EXISTS " + s + ".ap_no_partitions CASCADE")
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
                    + "  id INTEGER PRIMARY KEY,"
                    + "  val INTEGER"
                    + ");"
                    + "INSERT INTO " + s + ".ap_explicit_n SELECT"
                    + "  g, g * 10 FROM generate_series(1, 100) g")
                .cleanupSql("DROP TABLE IF EXISTS " + s + ".ap_explicit_n CASCADE")
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
                    + "  id INTEGER PRIMARY KEY,"
                    + "  val INTEGER"
                    + ");"
                    + "INSERT INTO " + s + ".ap_manual_bounds SELECT"
                    + "  g, g * 10 FROM generate_series(1, 50) g")
                .cleanupSql("DROP TABLE IF EXISTS " + s + ".ap_manual_bounds CASCADE")
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
                    + "  id INTEGER PRIMARY KEY,"
                    + "  val INTEGER"
                    + ");"
                    + "INSERT INTO " + s + ".ap_none VALUES (1, 10), (2, 20)")
                .cleanupSql("DROP TABLE IF EXISTS " + s + ".ap_none CASCADE")
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
                    + "  id INTEGER NOT NULL,"
                    + "  val INTEGER NOT NULL,"
                    + "  PRIMARY KEY (id)"
                    + ") PARTITION BY RANGE (id);"
                    + "CREATE TABLE " + s + ".ap_explicit_ignore_p1"
                    + "  PARTITION OF " + s + ".ap_explicit_ignore"
                    + "  FOR VALUES FROM (MINVALUE) TO (100);"
                    + "CREATE TABLE " + s + ".ap_explicit_ignore_p2"
                    + "  PARTITION OF " + s + ".ap_explicit_ignore"
                    + "  FOR VALUES FROM (100) TO (MAXVALUE);"
                    + "INSERT INTO " + s + ".ap_explicit_ignore SELECT"
                    + "  g, g FROM generate_series(1, 100) g")
                .cleanupSql("DROP TABLE IF EXISTS " + s + ".ap_explicit_ignore CASCADE")
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
                    + "  id INTEGER NOT NULL,"
                    + "  val INTEGER NOT NULL,"
                    + "  PRIMARY KEY (id)"
                    + ") PARTITION BY RANGE (id);"
                    + "CREATE TABLE " + s + ".ap_split_inherit_p1"
                    + "  PARTITION OF " + s + ".ap_split_inherit"
                    + "  FOR VALUES FROM (MINVALUE) TO (100);"
                    + "CREATE TABLE " + s + ".ap_split_inherit_p2"
                    + "  PARTITION OF " + s + ".ap_split_inherit"
                    + "  FOR VALUES FROM (100) TO (200);"
                    + "CREATE TABLE " + s + ".ap_split_inherit_p3"
                    + "  PARTITION OF " + s + ".ap_split_inherit"
                    + "  FOR VALUES FROM (200) TO (MAXVALUE);"
                    + "INSERT INTO " + s + ".ap_split_inherit SELECT"
                    + "  g, g FROM generate_series(1, 300) g")
                .cleanupSql("DROP TABLE IF EXISTS " + s + ".ap_split_inherit CASCADE")
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
                    + "  id INTEGER NOT NULL,"
                    + "  val INTEGER NOT NULL,"
                    + "  PRIMARY KEY (id)"
                    + ") PARTITION BY RANGE (id);"
                    + "CREATE TABLE " + s + ".ap_auto_split_count_p1"
                    + "  PARTITION OF " + s + ".ap_auto_split_count"
                    + "  FOR VALUES FROM (MINVALUE) TO (100);"
                    + "CREATE TABLE " + s + ".ap_auto_split_count_p2"
                    + "  PARTITION OF " + s + ".ap_auto_split_count"
                    + "  FOR VALUES FROM (100) TO (200);"
                    + "CREATE TABLE " + s + ".ap_auto_split_count_p3"
                    + "  PARTITION OF " + s + ".ap_auto_split_count"
                    + "  FOR VALUES FROM (200) TO (MAXVALUE);"
                    + "INSERT INTO " + s + ".ap_auto_split_count SELECT"
                    + "  g, g FROM generate_series(1, 300) g")
                .cleanupSql("DROP TABLE IF EXISTS " + s + ".ap_auto_split_count CASCADE")
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
                    + "  id INTEGER PRIMARY KEY,"
                    + "  val INTEGER"
                    + ");"
                    + "INSERT INTO " + s + ".ap_split_by_auto SELECT"
                    + "  g, g FROM generate_series(1, 40) g")
                .cleanupSql("DROP TABLE IF EXISTS " + s + ".ap_split_by_auto CASCADE")
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
                    + "  id INTEGER PRIMARY KEY,"
                    + "  val INTEGER"
                    + ");"
                    + "INSERT INTO " + s + ".ap_split_no_inherit VALUES"
                    + "  (1, 10), (2, 20), (3, 30)")
                .cleanupSql("DROP TABLE IF EXISTS " + s + ".ap_split_no_inherit CASCADE")
                .ydbPartitionCount(TableRef.AUTO)
                .splitBy("id")
                .splitCount(TableRef.AUTO)
                .expectPrimaryKey("id")
                .expectRowCount(3)
                .expectPartitionCount(1)
                .run();
    }
}
