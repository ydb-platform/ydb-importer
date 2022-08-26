package ydb.importer.source;

import ydb.importer.config.*;

/**
 *
 * @author zinal
 */
public class TableMapRunner {
    
    private final TableMap tableMap;
    private final NameChecker schemaDecision;
    private final NameChecker tableDecision;

    public TableMapRunner(TableMap tableMap) {
        this.tableMap = tableMap;
        this.schemaDecision = new NameChecker(
                tableMap.getIncludeSchemas(), 
                tableMap.getExcludeSchemas());
        this.tableDecision = new NameChecker(
                tableMap.getIncludeTables(), 
                tableMap.getExcludeTables());
    }

    public TableMap getTableMap() {
        return tableMap;
    }
    
    public TableOptions getOptions() {
        return tableMap.getOptions();
    }
    
    public boolean schemaMatches(String value) {
        return schemaDecision.decide(value);
    }
    
    public boolean tableMatches(String value) {
        return tableDecision.decide(value);
    }
    
}
