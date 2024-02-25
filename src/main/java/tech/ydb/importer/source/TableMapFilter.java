package tech.ydb.importer.source;

import tech.ydb.importer.config.*;

/**
 * A filtering logic for the tables retrieved from the source.
 * 
 * @author zinal
 */
public class TableMapFilter {
    
    private final TableMap tableMap;
    private final NameChecker schemaDecision;
    private final NameChecker tableDecision;

    public TableMapFilter(TableMap tableMap) {
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
