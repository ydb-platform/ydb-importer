package tech.ydb.importer.source;

import java.util.ArrayList;
import java.util.List;
import tech.ydb.importer.config.*;

/**
 * A collection of table mappings for table list retrieval and processing.
 *
 * @author zinal
 */
public class TableMapList {
    
    private final ImporterConfig config;
    private final List<TableMapFilter> maps;

    public TableMapList(ImporterConfig config) {
        this.config = config;
        this.maps = new ArrayList<>();
        for (TableMap tm : config.getTableMaps()) {
            this.maps.add(new TableMapFilter(tm));
        }
    }

    public ImporterConfig getConfig() {
        return config;
    }

    public List<TableMapFilter> getMaps() {
        return maps;
    }
    
    public List<TableRef> getRefs() {
        return config.getTableRefs();
    }

}
