package ydb.importer.target;

import org.junit.Test;
import ydb.importer.TableDecision;
import ydb.importer.source.*;
import ydb.importer.config.*;

/**
 *
 * @author zinal
 */
public class YdbTableBuilderTest {

    private TableDecision makeDecision1() {
        final TableOptions nf = new TableOptions("nf1", "path/to/dir/${schema}_${table}");
        final TableRef tr = new TableRef();
        tr.setSchema("schema1");
        tr.setTable("table1");
        tr.setOptions(nf);
        tr.getKeyNames().add("id_ref");
        tr.getKeyNames().add("id_main");
        final TableMetadata tm = new TableMetadata();
        tm.addColumn("id_ref", java.sql.Types.NUMERIC, 9, 0);
        tm.addColumn("id_main", java.sql.Types.NUMERIC, 15, 0);
        tm.addColumn("owner", java.sql.Types.VARCHAR, 50, 0);
        tm.addColumn("created_at", java.sql.Types.TIMESTAMP, 0, 0);
        tm.addColumn("updated_at", java.sql.Types.TIMESTAMP, 0, 6);
        tm.addColumn("total_acc", java.sql.Types.NUMERIC, 38, 2);
        tm.addColumn("amount", java.sql.Types.NUMERIC, 0, -127);
        tm.addColumn("datum", java.sql.Types.BLOB, 0, 0);
        tm.addKeys(tr.getKeyNames());
        final TableDecision td = new TableDecision(tr);
        td.setFailure(false);
        td.setMetadata(tm);
        return td;
    }

    @Test
    public void check() throws Exception {
        final TableDecision td = makeDecision1();
        new YdbTableBuilder(td).build();
        System.out.println("-- " + td.getTarget().getFullName());
        System.out.println(td.getTarget().getYqlScript());
    }

}
