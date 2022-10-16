package tech.ydb.importer.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.jdom2.Element;
import static tech.ydb.importer.config.JdomHelper.*;

/**
 *
 * @author zinal
 */
public class ImporterConfig {
    
    public static final org.slf4j.Logger LOG = 
            org.slf4j.LoggerFactory.getLogger(ImporterConfig.class);
    
    private WorkerConfig workers;
    private SourceConfig source;
    private TargetConfig target;
    private final List<TableMap> tableMaps = new ArrayList<>();
    private final List<TableRef> tableRefs = new ArrayList<>();
    private final Map<String, TableOptions> optionsMap = new HashMap<>();
    
    public ImporterConfig() {
        this.workers = new WorkerConfig();
        this.source = new SourceConfig();
        this.target = new TargetConfig();
    }
    
    public ImporterConfig(Element c) {
        this.workers = new WorkerConfig(getSingleChild(c, "workers"));
        this.source = new SourceConfig(getSingleChild(c, "source"));
        this.target = new TargetConfig(getSingleChild(c, "target"));
        for (Element cnf : getSomeChildren(c, "table-options")) {
            TableOptions nf = new TableOptions(cnf);
            optionsMap.put(nf.getName(), nf);
        }
        for (Element ctm : getChildren(c, "table-map")) {
            tableMaps.add(new TableMap(ctm, this.optionsMap));
        }
        for (Element ctr : getChildren(c, "table-ref")) {
            tableRefs.add(new TableRef(ctr, this.optionsMap));
        }
    }

    public WorkerConfig getWorkers() {
        return workers;
    }

    public void setWorkers(WorkerConfig workers) {
        this.workers = workers;
    }

    public SourceConfig getSource() {
        return source;
    }

    public void setSource(SourceConfig source) {
        this.source = source;
    }

    public TargetConfig getTarget() {
        return target;
    }

    public void setTarget(TargetConfig target) {
        this.target = target;
    }

    public List<TableMap> getTableMaps() {
        return tableMaps;
    }

    public List<TableRef> getTableRefs() {
        return tableRefs;
    }

    public Map<String, TableOptions> getOptionsMap() {
        return optionsMap;
    }

    public boolean validate() {
        boolean retval = true;
        if (source==null) {
            LOG.warn("Source configuration is not defined");
            retval = false;
        }
        if (target==null) {
            LOG.warn("Target configuration is not defined");
            retval = false;
        }
        if (tableMaps.isEmpty() && tableRefs.isEmpty()) {
            LOG.warn("Need to have at least one table map or table reference");
            retval = false;
        }
        return retval;
    }

    public boolean hasTarget() {
        if (target==null)
            return false;
        if (isBlank(target.getConnectionString()))
            return false;
        return true;
    }
}
