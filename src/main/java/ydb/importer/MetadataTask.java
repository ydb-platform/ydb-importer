package ydb.importer;

import java.sql.Connection;
import java.util.concurrent.Callable;
import ydb.importer.source.TableMetadata;

/**
 * Async task to grab the metadata for the single table from the source database.
 * @author zinal
 */
public class MetadataTask implements Callable<MetadataTask.Out> {

    private final static org.slf4j.Logger LOG = org.slf4j.LoggerFactory
            .getLogger(MetadataTask.class);
    
    private final YdbImporter owner;
    private final TableDecision td;

    public MetadataTask(YdbImporter owner, TableDecision td) {
        this.owner = owner;
        this.td = td;
    }

    @Override
    public Out call() throws Exception {
        try (Connection con = owner.getSourceCP().getConnection()) {
            TableMetadata tm = owner.getTableLister().readMetadata(con, td);
            return new Out(td, tm);
        } catch(Throwable ex) {
            LOG.error("Metadata retrieval failure for table {}.{}", 
                    td.getSchema(), td.getTable(), ex);
        }
        return new Out(td, null);
    }
    
    public static final class Out {

        public final TableDecision td;
        public final TableMetadata tm;

        public Out(TableDecision td, TableMetadata tm) {
            this.td = td;
            this.tm = tm;
        }
        
        public boolean isSuccess() {
            return (tm != null) && tm.isValid();
        }
    }

}
