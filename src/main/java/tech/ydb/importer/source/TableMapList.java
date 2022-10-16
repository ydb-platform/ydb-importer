package tech.ydb.importer.source;

import java.util.ArrayList;
import java.util.List;
import tech.ydb.importer.config.*;

/**
 *
 * @author zinal
 */
public class TableMapList {
    
    private final ImporterConfig config;
    private final List<TableMapRunner> maps;

    public TableMapList(ImporterConfig config) {
        this.config = config;
        this.maps = new ArrayList<>();
        for (TableMap tm : config.getTableMaps()) {
            this.maps.add(new TableMapRunner(tm));
        }
    }

    public ImporterConfig getConfig() {
        return config;
    }

    public List<TableMapRunner> getMaps() {
        return maps;
    }
    
    public List<TableRef> getRefs() {
        return config.getTableRefs();
    }

}
