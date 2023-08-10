package tech.ydb.importer.target;

import java.io.BufferedWriter;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.text.StringSubstitutor;
import tech.ydb.table.values.*;
import tech.ydb.importer.source.ColumnInfo;
import tech.ydb.importer.TableDecision;

/**
 *
 * @author zinal
 */
public class YdbTableBuilder {
    
    public static final String EOL = System.getProperty("line.separator");
    private final TableDecision tab;

    public YdbTableBuilder(TableDecision tab) {
        this.tab = tab;
    }

    public void build() {
        tab.setTarget(buildMainTable());
        tab.getBlobTargets().clear();
        for (ColumnInfo ci : tab.getMetadata().getColumns()) {
            if (ci.isBlob()) {
                tab.getBlobTargets().put( ci.getName(),
                        buildBlobTable(tab.getTarget(), ci) );
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
            } catch(Exception ex) {
                throw new RuntimeException("Cannot convert type for column [" + ci.getName()
                        + "] of table [" + fullName + "]", ex);
            }
            types.put(ci.getName(), type.makeOptional());
            sb.append("  `").append(ci.getName()).append("` ");
            sb.append(type.toString());
            sb.append(",").append(EOL);
        }
        if (tab.getMetadata().getKey().isEmpty()) {
            addSyntheticKey(sb, types);
        } else {
            addPrimaryKey(sb);
        }
        sb.append(");").append(EOL);
        return new TargetTable(tab, fullName, sb.toString(), StructType.of(types));
    }

    private TargetTable buildBlobTable(TargetTable main, ColumnInfo ci) {
        final StringBuilder sb = new StringBuilder();
        final String fullName = makeBlobName(ci.getName());
        sb.append("CREATE TABLE `").append(fullName).append("` (").append(EOL);
        sb.append("  `id` Int64,").append(EOL);
        sb.append("  `pos` Int32,").append(EOL);
        sb.append("  `val` String,").append(EOL);
        sb.append("  PRIMARY KEY(`id`, `pos`));").append(EOL);
        return new TargetTable(tab, fullName, sb.toString(), BlobSaver.BLOB_ROW);
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
        }
        return new StringSubstitutor(m).replace(tab.getOptions().getBlobTemplate());
    }

    private Type convertType(ColumnInfo ci) {
        switch (ci.getSqlType()) {
            case java.sql.Types.BOOLEAN:
                return PrimitiveType.Bool;
            case java.sql.Types.SMALLINT:
                return PrimitiveType.Int32;
            case java.sql.Types.INTEGER:
                return PrimitiveType.Int32;
            case java.sql.Types.BIGINT:
                return PrimitiveType.Int64;
            case java.sql.Types.DECIMAL:
            case java.sql.Types.NUMERIC:
                if (ci.getSqlScale() < 0)
                    return PrimitiveType.Double;
                if (ci.getSqlScale() == 0) {
                    if (ci.getSqlPrecision() == 0)
                        return PrimitiveType.Double;
                    if (ci.getSqlPrecision() < 10)
                        return PrimitiveType.Int32;
                    if (ci.getSqlPrecision() < 20)
                        return PrimitiveType.Int64;
                    // TEMP: only DECIMAL(22,9) is supported for table columns.
                    // For now Int64 is the best substitute for DECIMAL(38,0).
                    return PrimitiveType.Int64;
                    /*
                    int prec = ci.getSqlPrecision();
                    if (prec > 35) prec = 35;
                    return "Decimal(" + String.valueOf(prec) + ",0)";
                    */
                } else {
                    /*
                    int prec = ci.getSqlPrecision();
                    if (prec > 35) prec = 35;
                    int scale = ci.getSqlScale();
                    if (scale > prec) scale = prec;
                    return "Decimal(" + String.valueOf(prec) + "," + String.valueOf(scale) + ")";
                    */
                    // TEMP: only DECIMAL(22,9) is supported for table columns.
                    return DecimalType.of(22, 9);
                }
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
                    case DATE:
                        return PrimitiveType.Date;
                    case INT:
                        return PrimitiveType.Int32;
                    case STR:
                        return PrimitiveType.Text;
                }
                return PrimitiveType.Date;
            case java.sql.Types.TIMESTAMP:
                if (ci.getSqlScale()==0)
                    return PrimitiveType.Datetime;
                return PrimitiveType.Timestamp;
        }
        throw new IllegalArgumentException("Unsupported type code: " + ci.getSqlType());
    }

    private void addSyntheticKey(StringBuilder sb, Map<String, Type> types) {
        types.put(TargetTable.SYNTH_KEY_FIELD, PrimitiveType.Bytes);
        sb.append("  ydb_synth_key String,").append(EOL)
                .append("  PRIMARY KEY (ydb_synth_key)");
    }

    private void addPrimaryKey(StringBuilder sb) {
        sb.append("  PRIMARY KEY (");
        boolean comma = false;
        for (ColumnInfo ci : tab.getMetadata().getKey()) {
            if (comma) sb.append(", "); else comma = true;
            sb.append('`').append(ci.getName()).append('`');
        }
        sb.append(")");
    }

    public static void appendTo(BufferedWriter bw, TargetTable yt) throws Exception {
        bw.append("-- ");
        bw.append(yt.getFullName());
        bw.append(EOL);
        bw.append(yt.getYqlScript());
        bw.append(EOL);
    }

}
