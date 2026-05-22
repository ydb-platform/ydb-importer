package tech.ydb.importer.target;

import java.io.BufferedWriter;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.text.StringSubstitutor;

import tech.ydb.importer.TableDecision;
import tech.ydb.importer.config.TableOptions;
import tech.ydb.importer.source.ColumnInfo;
import tech.ydb.importer.source.YdbPartitioning;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.Type;

/**
 *
 * @author zinal
 */
public class YdbTableBuilder {

    private static final org.slf4j.Logger LOG
            = org.slf4j.LoggerFactory.getLogger(YdbTableBuilder.class);

    public static final String EOL = System.getProperty("line.separator");
    private final TableDecision tab;

    public YdbTableBuilder(TableDecision tab) {
        this.tab = tab;
    }

    public void build() {
        tab.getBlobTargets().clear();
        tab.getClobTargets().clear();
        for (ColumnInfo ci : tab.getMetadata().getColumns()) {
            if (ci.isBlob()) {
                if (isClobOverride(ci.getName())) {
                    LOG.warn("ignoring user defined CLOB column {} in table {}: column is BLOB",
                            ci.getName(), makeTableName());
                }
                tab.getBlobTargets().put(ci.getName(),
                        buildAuxTable(ci, "String", BlobReader.BLOB_ROW));
            } else if (ci.isClob()) {
                tab.getClobTargets().put(ci.getName(),
                        buildAuxTable(ci, "Text", ClobReader.CLOB_ROW));
            } else if (isClobOverride(ci.getName())) {
                validateClobOverride(ci);
                ci.setSqlType(java.sql.Types.CLOB);
                tab.getClobTargets().put(ci.getName(),
                        buildAuxTable(ci, "Text", ClobReader.CLOB_ROW));
            }
        }

        TargetTable tt = buildMainTable();
        if (tt != null) {
            tab.setTarget(tt);
        }
    }

    private boolean isClobOverride(String columnName) {
        return tab.getTableRef() != null && tab.getTableRef().isClobColumn(columnName);
    }

    private void validateClobOverride(ColumnInfo ci) {
        Type type = YdbTypeMapper.convertType(ci, tab.getOptions());
        if (!PrimitiveType.Text.equals(type)) {
            throw new RuntimeException("clob-column " + ci.getName()
                    + " in table " + makeTableName() + " maps to " + type + ", not Text");
        }
    }

    private TargetTable buildMainTable() {
        final Map<String, Type> types = new HashMap<>();
        final StringBuilder sb = new StringBuilder();
        final String fullName = makeTableName();
        sb.append("CREATE TABLE ");
        sb.append("`").append(fullName).append("`");
        sb.append(" (").append(EOL);
        for (ColumnInfo ci : tab.getMetadata().getColumns()) {
            Type type;
            try {
                type = YdbTypeMapper.convertType(ci, tab.getOptions());
            } catch (Exception ex) {
                throw new RuntimeException("Cannot convert type for column [" + ci.getName()
                        + "] of table [" + fullName + "]", ex);
            }
            if (type == null) {
                // unknown type, and skip mode is enabled
                LOG.warn("Skipped column {} in table {}.{} due to unknown type {}",
                        ci.getName(), tab.getSchema(), tab.getTable(), ci.getSqlType());
                continue;
            }
            sb.append("  `").append(ci.getDestinationName()).append("` ");
            sb.append(type.toString());
            if (ci.isNullable()) {
                types.put(ci.getDestinationName(), type.makeOptional());
            } else {
                types.put(ci.getDestinationName(), type);
                sb.append(" NOT NULL");
            }
            sb.append(",").append(EOL);
        }
        if (types.isEmpty()) {
            // unknown type, and skip mode is enabled
            LOG.warn("No columns are defined in table {}.{} - SKIPPING",
                    tab.getSchema(), tab.getTable());
            return null;
        }
        if (tab.getMetadata().getKey().isEmpty()) {
            addSyntheticKey(sb, types);
        } else {
            addPrimaryKey(sb);
        }
        sb.append(")").append(EOL);
        appendPartitioning(sb);
        return new TargetTable(tab, fullName, sb.toString(), StructType.of(types));
    }

    private void appendPartitioning(StringBuilder sb) {
        final YdbPartitioning part = tab.getMetadata().getYdbPartitioning();
        final boolean columnStore =
                TableOptions.StoreType.COLUMN.equals(tab.getOptions().getStoreType());
        if (columnStore && part.isHash()) {
            sb.append("PARTITION BY HASH(`").append(part.getHashColumn()).append("`)").append(EOL);
        }
        sb.append("WITH (").append(EOL);
        if (columnStore) {
            sb.append("  STORE = COLUMN").append(EOL);
            if (part.isHash()) {
                sb.append(", AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = ")
                        .append(part.getHashPartitions()).append(EOL);
            }
        } else if (part.isKeyRange()) {
            appendAutoPartitioning(sb, part.getCuts().size() + 1);
            sb.append(", PARTITION_AT_KEYS = (")
                    .append(String.join(", ", part.getCuts())).append(")").append(EOL);
        } else {
            appendAutoPartitioning(sb, 9999);
        }
        sb.append(");").append(EOL);
    }

    private void appendAutoPartitioning(StringBuilder sb, int minPartitions) {
        sb.append("  AUTO_PARTITIONING_BY_SIZE = ENABLED").append(EOL);
        sb.append(", AUTO_PARTITIONING_BY_LOAD = ENABLED").append(EOL);
        sb.append(", AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = ").append(minPartitions).append(EOL);
        sb.append(", AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 9999").append(EOL);
    }

    private TargetTable buildAuxTable(ColumnInfo ci, String valType, StructType rowType) {
        final StringBuilder sb = new StringBuilder();
        final String fullName = makeBlobName(ci.getDestinationName());
        sb.append("CREATE TABLE `").append(fullName).append("` (").append(EOL);
        sb.append("  `id` Int64,").append(EOL);
        sb.append("  `pos` Int32,").append(EOL);
        sb.append("  `val` ").append(valType).append(",").append(EOL);
        sb.append("  PRIMARY KEY(`id`, `pos`));").append(EOL);
        return new TargetTable(tab, fullName, sb.toString(), rowType);
    }

    private String makeTableName() {
        final Map<String, String> m = new HashMap<>();
        switch (tab.getOptions().getCaseMode()) {
            case ASIS:
                m.put("schema", tab.getSchema());
                m.put("table", tab.getTable());
                break;
            case LOWER:
                m.put("schema", tab.getSchema().toLowerCase());
                m.put("table", tab.getTable().toLowerCase());
                break;
            case UPPER:
                m.put("schema", tab.getSchema().toUpperCase());
                m.put("table", tab.getTable().toUpperCase());
                break;
            default: {
                /* noop */
            }
        }
        return new StringSubstitutor(m).replace(tab.getOptions().getMainTemplate());
    }

    private String makeBlobName(String field) {
        final Map<String, String> m = new HashMap<>();
        switch (tab.getOptions().getCaseMode()) {
            case ASIS:
                m.put("schema", tab.getSchema());
                m.put("table", tab.getTable());
                m.put("field", field);
                break;
            case LOWER:
                m.put("schema", tab.getSchema().toLowerCase());
                m.put("table", tab.getTable().toLowerCase());
                m.put("field", field.toLowerCase());
                break;
            case UPPER:
                m.put("schema", tab.getSchema().toUpperCase());
                m.put("table", tab.getTable().toUpperCase());
                m.put("field", field.toUpperCase());
                break;
            default: {
                /* noop */
            }
        }
        return new StringSubstitutor(m).replace(tab.getOptions().getBlobTemplate());
    }

    private void addSyntheticKey(StringBuilder sb, Map<String, Type> types) {
        String field = TargetTable.SYNTH_KEY_FIELD;
        types.put(field, PrimitiveType.Text);
        sb.append("  ").append(field).append(" Text NOT NULL,").append(EOL)
                .append("  PRIMARY KEY (").append(field).append(")").append(EOL);
    }

    private void addPrimaryKey(StringBuilder sb) {
        sb.append("  PRIMARY KEY (");
        boolean comma = false;
        for (ColumnInfo ci : tab.getMetadata().getKey()) {
            if (comma) {
                sb.append(", ");
            } else {
                comma = true;
            }
            sb.append('`').append(ci.getDestinationName()).append('`');
        }
        sb.append(")").append(EOL);
    }

    public static void appendTo(BufferedWriter bw, TargetTable yt) throws Exception {
        bw.append("-- ");
        bw.append(yt.getFullName());
        bw.append(EOL);
        bw.append(yt.getYqlScript());
        bw.append(EOL);
    }

}
