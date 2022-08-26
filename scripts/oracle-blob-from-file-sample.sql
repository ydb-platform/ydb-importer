
alter session set container=pdb1;

CREATE OR REPLACE DIRECTORY BLOB_DIR AS '/home/oracle/datum';

CREATE TABLE demo1.blobtab1 (
  A INTEGER NOT NULL PRIMARY KEY,
  B VARCHAR(100),
  C BLOB,
  D TIMESTAMP );


DECLARE
  filename VARCHAR2(50) := 'libdbtools19.a';
  lb BLOB;
  lf BFILE := BFILENAME('BLOB_DIR', filename);
BEGIN
  INSERT INTO demo1.blobtab1 (a, b, c, d)
    VALUES (1001, filename, EMPTY_BLOB(), SYSTIMESTAMP)
    RETURNING c INTO lb;

    DBMS_LOB.OPEN(lf, DBMS_LOB.LOB_READONLY);
    DBMS_LOB.OPEN(lb, DBMS_LOB.LOB_READWRITE);
    DBMS_LOB.LOADFROMFILE(DEST_LOB => lb,
                          SRC_LOB  => lf,
                          AMOUNT   => DBMS_LOB.GETLENGTH(lf));
    DBMS_LOB.CLOSE(lf);
    DBMS_LOB.CLOSE(lb);
    COMMIT;
END;
/


DECLARE
  filename VARCHAR2(50) := 'libeva19.a';
  lb BLOB;
  lf BFILE := BFILENAME('BLOB_DIR', filename);
BEGIN
  INSERT INTO demo1.blobtab1 (a, b, c, d)
    VALUES (1002, filename, EMPTY_BLOB(), SYSTIMESTAMP)
    RETURNING c INTO lb;

    DBMS_LOB.OPEN(lf, DBMS_LOB.LOB_READONLY);
    DBMS_LOB.OPEN(lb, DBMS_LOB.LOB_READWRITE);
    DBMS_LOB.LOADFROMFILE(DEST_LOB => lb,
                          SRC_LOB  => lf,
                          AMOUNT   => DBMS_LOB.GETLENGTH(lf));
    DBMS_LOB.CLOSE(lf);
    DBMS_LOB.CLOSE(lb);
    COMMIT;
END;
/

DECLARE
  filename VARCHAR2(50) := 'somefile.zip';
  lb BLOB;
  lf BFILE := BFILENAME('BLOB_DIR', filename);
BEGIN
  INSERT INTO demo1.blobtab1 (a, b, c, d)
    VALUES (2001, filename, EMPTY_BLOB(), SYSTIMESTAMP)
    RETURNING c INTO lb;

    DBMS_LOB.OPEN(lf, DBMS_LOB.LOB_READONLY);
    DBMS_LOB.OPEN(lb, DBMS_LOB.LOB_READWRITE);
    DBMS_LOB.LOADFROMFILE(DEST_LOB => lb,
                          SRC_LOB  => lf,
                          AMOUNT   => DBMS_LOB.GETLENGTH(lf));
    DBMS_LOB.CLOSE(lf);
    DBMS_LOB.CLOSE(lb);
    COMMIT;
END;
/

DECLARE
  filename VARCHAR2(50) := 'somefile.tar.xz';
  lb BLOB;
  lf BFILE := BFILENAME('BLOB_DIR', filename);
BEGIN
  INSERT INTO demo1.blobtab1 (a, b, c, d)
    VALUES (2002, filename, EMPTY_BLOB(), SYSTIMESTAMP)
    RETURNING c INTO lb;

    DBMS_LOB.OPEN(lf, DBMS_LOB.LOB_READONLY);
    DBMS_LOB.OPEN(lb, DBMS_LOB.LOB_READWRITE);
    DBMS_LOB.LOADFROMFILE(DEST_LOB => lb,
                          SRC_LOB  => lf,
                          AMOUNT   => DBMS_LOB.GETLENGTH(lf));
    DBMS_LOB.CLOSE(lf);
    DBMS_LOB.CLOSE(lb);
    COMMIT;
END;
/
