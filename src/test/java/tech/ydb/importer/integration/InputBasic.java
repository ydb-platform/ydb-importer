package tech.ydb.importer.integration;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author zinal
 */
public class InputBasic implements InputAny {

    @Override
    public List<String> getCreate() {
        List<String> sql = new ArrayList<>();
        sql.add("CREATE SCHEMA schema1;");
        sql.add("CREATE SCHEMA schema2;");
        sql.add("CREATE SCHEMA schema3;");
        sql.add("CREATE TABLE schema1.table1 ("
                + "a INTEGER NOT NULL, "
                + "b VARCHAR(100) NOT NULL, "
                + "c BIGINT NOT NULL, "
                + "PRIMARY KEY(a));");
        sql.add("CREATE TABLE schema2.table2 ("
                + "a INTEGER NOT NULL, "
                + "b VARCHAR(100) NOT NULL, "
                + "c BIGINT NOT NULL, "
                + "PRIMARY KEY(a));");
        sql.add("CREATE TABLE schema3.table3 ("
                + "a INTEGER NOT NULL, "
                + "b VARCHAR(100) NOT NULL, "
                + "c BIGINT NOT NULL, "
                + "PRIMARY KEY(a));");
        return sql;
    }

    @Override
    public List<String> getDrop() {
        List<String> sql = new ArrayList<>();
        sql.add("DROP TABLE schema1.table1;");
        sql.add("DROP TABLE schema2.table2;");
        sql.add("DROP TABLE schema3.table3;");
        sql.add("DROP SCHEMA schema1;");
        sql.add("DROP SCHEMA schema2;");
        sql.add("DROP SCHEMA schema3;");
        return sql;
    }

    @Override
    public List<String> getInsert() {
        List<String> sql = new ArrayList<>();
        sql.add("INSERT INTO schema1.table1(a,b,c) VALUES"
                + " (1, 'One', 10001)");
        sql.add("INSERT INTO schema2.table2(a,b,c) VALUES"
                + " (1, 'One', 10001)"
                + ",(2, 'Two', 21002)");
        sql.add("INSERT INTO schema3.table3(a,b,c) VALUES"
                + " (1, 'One', 10001)"
                + ",(2, 'Two', 22002)"
                + ",(3, 'Three', 32003)");
        return sql;
    }

}
