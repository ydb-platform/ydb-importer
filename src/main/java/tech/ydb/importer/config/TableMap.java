package tech.ydb.importer.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.jdom2.Element;

/**
 * Table map filters the tables in the source system through the names of their schemas and of the
 * tables themselves.
 *
 * @author zinal
 */
public class TableMap extends JdomHelper {

    private TableOptions options;
    private final List<NameFilter> includeSchemas = new ArrayList<>();
    private final List<NameFilter> includeTables = new ArrayList<>();
    private final List<NameFilter> excludeSchemas = new ArrayList<>();
    private final List<NameFilter> excludeTables = new ArrayList<>();

    public TableMap() {
    }

    public TableMap(Element c, Map<String, TableOptions> optionsMap) {
        this.options = optionsMap.get(getAttr(c, "options"));
        if (this.options == null) {
            throw raiseIllegal(c, "options");
        }
        for (Element cnf : getChildren(c, "include-schemas")) {
            includeSchemas.add(new NameFilter(cnf));
        }
        for (Element cnf : getChildren(c, "exclude-schemas")) {
            excludeSchemas.add(new NameFilter(cnf));
        }
        for (Element cnf : getChildren(c, "include-tables")) {
            includeTables.add(new NameFilter(cnf));
        }
        for (Element cnf : getChildren(c, "exclude-tables")) {
            excludeTables.add(new NameFilter(cnf));
        }
        if (includeSchemas.isEmpty()) {
            includeSchemas.add(new NameFilter(".*", true, false));
        }
        if (includeTables.isEmpty()) {
            includeTables.add(new NameFilter(".*", true, false));
        }
    }

    public TableOptions getOptions() {
        return options;
    }

    public void setOptions(TableOptions nf) {
        this.options = nf;
    }

    public List<NameFilter> getIncludeSchemas() {
        return includeSchemas;
    }

    public List<NameFilter> getIncludeTables() {
        return includeTables;
    }

    public List<NameFilter> getExcludeSchemas() {
        return excludeSchemas;
    }

    public List<NameFilter> getExcludeTables() {
        return excludeTables;
    }

}
