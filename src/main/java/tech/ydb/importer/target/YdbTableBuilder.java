package tech.ydb.importer.target;

import java.io.BufferedWriter;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.text.StringSubstitutor;

import tech.ydb.importer.TableDecision;
import tech.ydb.importer.config.TableOptions;
import tech.ydb.importer.source.ColumnInfo;
import tech.ydb.table.values.DecimalType;
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
        TargetTable tt = buildMainTable();
        if (tt != null) {
            tab.setTarget(tt);
            tab.getBlobTargets().clear();
            for (ColumnInfo ci : tab.getMetadata().getColumns()) {
                if (ci.isBlob()) {
                    tab.getBlobTargets().put(ci.getName(),
                            buildBlobTable(tab.getTarget(), ci));
                }
            }
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
            final Type type;
            try {
                type = convertType(ci);
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
        sb.append(") WITH (").append(EOL);
        if (TableOptions.StoreType.COLUMN.equals(tab.getOptions().getStoreType())) {
            sb.append("  STORE = COLUMN").append(EOL);
        } else {
            sb.append("  AUTO_PARTITIONING_BY_SIZE = ENABLED").append(EOL);
            sb.append(", AUTO_PARTITIONING_BY_LOAD = ENABLED").append(EOL);
            sb.append(", AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 9999").append(EOL);
            sb.append(", AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 9999").append(EOL);
        }
        sb.append(");").append(EOL);
        return new TargetTable(tab, fullName, sb.toString(), StructType.of(types));
    }

    private TargetTable buildBlobTable(TargetTable main, ColumnInfo ci) {
        final StringBuilder sb = new StringBuilder();
        final String fullName = makeBlobName(ci.getDestinationName());
        sb.append("CREATE TABLE `").append(fullName).append("` (").append(EOL);
        sb.append("  `id` Int64,").append(EOL);
        sb.append("  `pos` Int32,").append(EOL);
        sb.append("  `val` String,").append(EOL);
        sb.append("  PRIMARY KEY(`id`, `pos`));").append(EOL);
        return new TargetTable(tab, fullName, sb.toString(), BlobReader.BLOB_ROW);
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

    private Type convertType(ColumnInfo ci) {
        switch (ci.getSqlType()) {
            case java.sql.Types.BOOLEAN:
            case java.sql.Types.BIT:
                return PrimitiveType.Bool;
            case java.sql.Types.TINYINT:
                return PrimitiveType.Int32;
            case java.sql.Types.SMALLINT:
                return PrimitiveType.Int32;
            case java.sql.Types.INTEGER:
                return PrimitiveType.Int32;
            case java.sql.Types.BIGINT:
                return PrimitiveType.Int64;
            case java.sql.Types.DECIMAL:
            case java.sql.Types.NUMERIC:
                if (ci.getSqlScale() < 0) {
                    return PrimitiveType.Double;
                }
                if (ci.getSqlScale() == 0) {
                    if (ci.getSqlPrecision() == 0) {
                        return PrimitiveType.Double;
                    }
                    if (ci.getSqlPrecision() < 10) {
                        return PrimitiveType.Int32;
                    }
                    if (ci.getSqlPrecision() < 20) {
                        return PrimitiveType.Int64;
                    }
                    if (tab.getOptions().isAllowCustomDecimal()) {
                        return DecimalType.of(35, 0);
                    }
                    return PrimitiveType.Int64;
                } else {
                    if (tab.getOptions().isAllowCustomDecimal()) {
                        int prec = ci.getSqlPrecision();
                        if (prec > 35) {
                            prec = 35;
                        }
                        int scale = ci.getSqlScale();
                        if (scale > prec) {
                            scale = prec;
                        }
                        return DecimalType.of(prec, scale);
                    }
                    return DecimalType.getDefault();
                }
            case java.sql.Types.DOUBLE:
                return PrimitiveType.Double;
            case java.sql.Types.FLOAT:
                return PrimitiveType.Float;
            case java.sql.Types.VARCHAR:
            case java.sql.Types.CHAR:
            case java.sql.Types.NVARCHAR:
            case java.sql.Types.NCHAR:
            case java.sql.Types.LONGNVARCHAR:
            case java.sql.Types.LONGVARCHAR:
            case java.sql.Types.CLOB: // MAYBE: store in a separate table, like BLOBS
                return PrimitiveType.Text;
            case java.sql.Types.BINARY:
            case java.sql.Types.VARBINARY:
                return PrimitiveType.Bytes;
            case java.sql.Types.BLOB:
            case java.sql.Types.LONGVARBINARY:
            case java.sql.Types.SQLXML:
                return PrimitiveType.Int64; // Id of record sequence in the separate table.
            case java.sql.Types.DATE:
                switch (tab.getOptions().getDateConv()) {
                    case DATE_NEW:
                        return PrimitiveType.Date32;
                    case DATE:
                        return PrimitiveType.Date;
                    case INT:
                        return PrimitiveType.Int32;
                    case STR:
                        return PrimitiveType.Text;
                    default: {
                        /* noop */
                    }
                }
                return PrimitiveType.Date32;
            case java.sql.Types.TIME:
                return PrimitiveType.Int32;
            case java.sql.Types.TIMESTAMP:
                switch (tab.getOptions().getTimestampConv()) {
                    case DATE_NEW:
                        if (ci.getSqlScale() == 0) {
                            return PrimitiveType.Datetime64;
                        }
                        return PrimitiveType.Timestamp64;
                    case DATE:
                        if (ci.getSqlScale() == 0) {
                            return PrimitiveType.Datetime;
                        }
                        return PrimitiveType.Timestamp;
                    case INT:
                        return PrimitiveType.Uint64;
                    case STR:
                        return PrimitiveType.Text;
                    default: {
                        /* noop */
                    }
                }
                if (ci.getSqlScale() == 0) {
                    return PrimitiveType.Datetime64;
                }
                return PrimitiveType.Timestamp64;
            default: {
                /* noop */
            }
        }
        if (tab.getOptions().isSkipUnknownTypes()) {
            return null;
        }
        throw new IllegalArgumentException("Unsupported type code: " + ci.getSqlType());
    }

    private void addSyntheticKey(StringBuilder sb, Map<String, Type> types) {
        String field = TargetTable.SYNTH_KEY_FIELD;
        types.put(field, PrimitiveType.Text);
        sb.append("  ").append(field).append(" Text,").append(EOL)
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
