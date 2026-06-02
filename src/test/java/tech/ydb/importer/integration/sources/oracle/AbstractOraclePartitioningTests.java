package tech.ydb.importer.integration.sources.oracle;

import org.junit.jupiter.api.Test;

import tech.ydb.importer.config.TableRef;
import tech.ydb.importer.integration.tabletest.AbstractYdbImporterTableTest;

public abstract class AbstractOraclePartitioningTests
        extends AbstractYdbImporterTableTest {

    @Test
    public void mirrorSuccessful() throws Exception {
        String s = schemaName();
        tableTest(s, "AP_MIRROR_OK")
                .setupSql(
                    "CREATE TABLE " + s + ".AP_MIRROR_OK ("
                    + "  ID  NUMBER(10,0) NOT NULL,"
                    + "  VAL NUMBER(10,0) NOT NULL,"
                    + "  PRIMARY KEY (ID)"
                    + ") PARTITION BY RANGE (ID) ("
                    + "  PARTITION P1 VALUES LESS THAN (100),"
                    + "  PARTITION P2 VALUES LESS THAN (200),"
                    + "  PARTITION P3 VALUES LESS THAN (MAXVALUE));"
                    + "INSERT INTO " + s + ".AP_MIRROR_OK VALUES (1, 10);"
                    + "INSERT INTO " + s + ".AP_MIRROR_OK VALUES (50, 20);"
                    + "INSERT INTO " + s + ".AP_MIRROR_OK VALUES (120, 30);"
                    + "INSERT INTO " + s + ".AP_MIRROR_OK VALUES (210, 40)")
                .cleanupSql("DROP TABLE " + s + ".AP_MIRROR_OK")
                .ydbPartitionCount(TableRef.AUTO)
                .expectPrimaryKey("ID")
                .expectRowCount(4)
                .expectPartitionCount(3)
                .run();
    }

    @Test
    public void mirrorSkippedOnOverlap() throws Exception {
        String s = schemaName();
        tableTest(s, "AP_MIRROR_OVERLAP")
                .setupSql(
                    "CREATE TABLE " + s + ".AP_MIRROR_OVERLAP ("
                    + "  ID     NUMBER(10,0) NOT NULL,"
                    + "  REGION NUMBER(10,0) NOT NULL,"
                    + "  PRIMARY KEY (ID, REGION)"
                    + ") PARTITION BY LIST (REGION) ("
                    + "  PARTITION P_A VALUES (1),"
                    + "  PARTITION P_B VALUES (2));"
                    + "INSERT INTO " + s + ".AP_MIRROR_OVERLAP VALUES (1, 1);"
                    + "INSERT INTO " + s + ".AP_MIRROR_OVERLAP VALUES (2, 2);"
                    + "INSERT INTO " + s + ".AP_MIRROR_OVERLAP VALUES (3, 1);"
                    + "INSERT INTO " + s + ".AP_MIRROR_OVERLAP VALUES (4, 2)")
                .cleanupSql("DROP TABLE " + s + ".AP_MIRROR_OVERLAP")
                .ydbPartitionCount(TableRef.AUTO)
                .expectPrimaryKey("ID", "REGION")
                .expectRowCount(4)
                .expectPartitionCount(1)
                .run();
    }

    @Test
    public void mirrorSkippedOnNonPartitioned() throws Exception {
        String s = schemaName();
        tableTest(s, "AP_NO_PARTITIONS")
                .setupSql(
                    "CREATE TABLE " + s + ".AP_NO_PARTITIONS ("
                    + "  ID  NUMBER(10,0) PRIMARY KEY,"
                    + "  VAL NUMBER(10,0));"
                    + "INSERT INTO " + s + ".AP_NO_PARTITIONS VALUES (1, 10);"
                    + "INSERT INTO " + s + ".AP_NO_PARTITIONS VALUES (2, 20);"
                    + "INSERT INTO " + s + ".AP_NO_PARTITIONS VALUES (3, 30)")
                .cleanupSql("DROP TABLE " + s + ".AP_NO_PARTITIONS")
                .ydbPartitionCount(TableRef.AUTO)
                .expectPrimaryKey("ID")
                .expectRowCount(3)
                .expectPartitionCount(1)
                .run();
    }

    @Test
    public void explicitNGlobalMinMax() throws Exception {
        String s = schemaName();
        tableTest(s, "AP_EXPLICIT_N")
                .setupSql(
                    "CREATE TABLE " + s + ".AP_EXPLICIT_N ("
                    + "  ID  NUMBER(10,0) PRIMARY KEY,"
                    + "  VAL NUMBER(10,0));"
                    + "INSERT INTO " + s + ".AP_EXPLICIT_N"
                    + "  SELECT LEVEL, LEVEL * 10 FROM dual"
                    + "  CONNECT BY LEVEL <= 100")
                .cleanupSql("DROP TABLE " + s + ".AP_EXPLICIT_N")
                .ydbPartitionCount(8)
                .expectPrimaryKey("ID")
                .expectRowCount(100)
                .expectPartitionCount(8)
                .run();
    }

    @Test
    public void explicitNManualBounds() throws Exception {
        String s = schemaName();
        tableTest(s, "AP_MANUAL_BOUNDS")
                .setupSql(
                    "CREATE TABLE " + s + ".AP_MANUAL_BOUNDS ("
                    + "  ID  NUMBER(10,0) PRIMARY KEY,"
                    + "  VAL NUMBER(10,0));"
                    + "INSERT INTO " + s + ".AP_MANUAL_BOUNDS"
                    + "  SELECT LEVEL, LEVEL * 10 FROM dual"
                    + "  CONNECT BY LEVEL <= 50")
                .cleanupSql("DROP TABLE " + s + ".AP_MANUAL_BOUNDS")
                .ydbPartitionCount(5)
                .ydbPartitionFrom("1")
                .ydbPartitionTo("100")
                .expectPrimaryKey("ID")
                .expectRowCount(50)
                .expectPartitionCount(5)
                .run();
    }

    @Test
    public void noneLiteralDisablesPreSplit() throws Exception {
        String s = schemaName();
        tableTest(s, "AP_NONE")
                .setupSql(
                    "CREATE TABLE " + s + ".AP_NONE ("
                    + "  ID  NUMBER(10,0) PRIMARY KEY,"
                    + "  VAL NUMBER(10,0));"
                    + "INSERT INTO " + s + ".AP_NONE VALUES (1, 10);"
                    + "INSERT INTO " + s + ".AP_NONE VALUES (2, 20)")
                .cleanupSql("DROP TABLE " + s + ".AP_NONE")
                .ydbPartitionCount(TableRef.NONE)
                .expectPrimaryKey("ID")
                .expectRowCount(2)
                .expectPartitionCount(1)
                .run();
    }

    @Test
    public void explicitNIgnoresSourcePartitions() throws Exception {
        String s = schemaName();
        tableTest(s, "AP_EXPLICIT_IGNORE")
                .setupSql(
                    "CREATE TABLE " + s + ".AP_EXPLICIT_IGNORE ("
                    + "  ID  NUMBER(10,0) NOT NULL,"
                    + "  VAL NUMBER(10,0) NOT NULL,"
                    + "  PRIMARY KEY (ID)"
                    + ") PARTITION BY RANGE (ID) ("
                    + "  PARTITION P1 VALUES LESS THAN (100),"
                    + "  PARTITION P2 VALUES LESS THAN (MAXVALUE));"
                    + "INSERT INTO " + s + ".AP_EXPLICIT_IGNORE"
                    + "  SELECT LEVEL, LEVEL FROM dual"
                    + "  CONNECT BY LEVEL <= 100")
                .cleanupSql("DROP TABLE " + s + ".AP_EXPLICIT_IGNORE")
                .ydbPartitionCount(8)
                .expectPrimaryKey("ID")
                .expectRowCount(100)
                .expectPartitionCount(8)
                .run();
    }

    @Test
    public void splitCountAutoInheritsMirror() throws Exception {
        String s = schemaName();
        tableTest(s, "AP_SPLIT_INHERIT")
                .setupSql(
                    "CREATE TABLE " + s + ".AP_SPLIT_INHERIT ("
                    + "  ID  NUMBER(10,0) NOT NULL,"
                    + "  VAL NUMBER(10,0) NOT NULL,"
                    + "  PRIMARY KEY (ID)"
                    + ") PARTITION BY RANGE (ID) ("
                    + "  PARTITION P1 VALUES LESS THAN (100),"
                    + "  PARTITION P2 VALUES LESS THAN (200),"
                    + "  PARTITION P3 VALUES LESS THAN (MAXVALUE));"
                    + "INSERT INTO " + s + ".AP_SPLIT_INHERIT"
                    + "  SELECT LEVEL, LEVEL FROM dual"
                    + "  CONNECT BY LEVEL <= 300")
                .cleanupSql("DROP TABLE " + s + ".AP_SPLIT_INHERIT")
                .ydbPartitionCount(TableRef.AUTO)
                .splitBy("ID")
                .splitCount(TableRef.AUTO)
                .useSourcePartitions(false)
                .expectPrimaryKey("ID")
                .expectRowCount(300)
                .expectPartitionCount(3)
                .run();
    }

    @Test
    public void autoWithExplicitSplitCount() throws Exception {
        String s = schemaName();
        tableTest(s, "AP_AUTO_SPLIT_COUNT")
                .setupSql(
                    "CREATE TABLE " + s + ".AP_AUTO_SPLIT_COUNT ("
                    + "  ID  NUMBER(10,0) NOT NULL,"
                    + "  VAL NUMBER(10,0) NOT NULL,"
                    + "  PRIMARY KEY (ID)"
                    + ") PARTITION BY RANGE (ID) ("
                    + "  PARTITION P1 VALUES LESS THAN (100),"
                    + "  PARTITION P2 VALUES LESS THAN (200),"
                    + "  PARTITION P3 VALUES LESS THAN (MAXVALUE));"
                    + "INSERT INTO " + s + ".AP_AUTO_SPLIT_COUNT"
                    + "  SELECT LEVEL, LEVEL FROM dual"
                    + "  CONNECT BY LEVEL <= 300")
                .cleanupSql("DROP TABLE " + s + ".AP_AUTO_SPLIT_COUNT")
                .ydbPartitionCount(TableRef.AUTO)
                .splitBy("ID")
                .splitCount(6)
                .expectPrimaryKey("ID")
                .expectRowCount(300)
                .expectPartitionCount(6)
                .run();
    }

    @Test
    public void splitByAutoResolvesLeadingPk() throws Exception {
        String s = schemaName();
        tableTest(s, "AP_SPLIT_BY_AUTO")
                .setupSql(
                    "CREATE TABLE " + s + ".AP_SPLIT_BY_AUTO ("
                    + "  ID  NUMBER(10,0) PRIMARY KEY,"
                    + "  VAL NUMBER(10,0));"
                    + "INSERT INTO " + s + ".AP_SPLIT_BY_AUTO"
                    + "  SELECT LEVEL, LEVEL FROM dual"
                    + "  CONNECT BY LEVEL <= 40")
                .cleanupSql("DROP TABLE " + s + ".AP_SPLIT_BY_AUTO")
                .ydbPartitionCount(TableRef.NONE)
                .splitBy(TableRef.SPLIT_BY_AUTO)
                .splitCount(4)
                .expectPrimaryKey("ID")
                .expectRowCount(40)
                .expectPartitionCount(1)
                .run();
    }

    @Test
    public void splitCountAutoNoResolvable() throws Exception {
        String s = schemaName();
        tableTest(s, "AP_SPLIT_NO_INHERIT")
                .setupSql(
                    "CREATE TABLE " + s + ".AP_SPLIT_NO_INHERIT ("
                    + "  ID  NUMBER(10,0) PRIMARY KEY,"
                    + "  VAL NUMBER(10,0));"
                    + "INSERT INTO " + s + ".AP_SPLIT_NO_INHERIT VALUES (1, 10);"
                    + "INSERT INTO " + s + ".AP_SPLIT_NO_INHERIT VALUES (2, 20);"
                    + "INSERT INTO " + s + ".AP_SPLIT_NO_INHERIT VALUES (3, 30)")
                .cleanupSql("DROP TABLE " + s + ".AP_SPLIT_NO_INHERIT")
                .ydbPartitionCount(TableRef.AUTO)
                .splitBy("ID")
                .splitCount(TableRef.AUTO)
                .expectPrimaryKey("ID")
                .expectRowCount(3)
                .expectPartitionCount(1)
                .run();
    }
}
