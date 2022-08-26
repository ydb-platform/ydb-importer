package ydb.importer.target;

import java.io.BufferedWriter;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.text.StringSubstitutor;
import com.yandex.ydb.table.values.*;
import ydb.importer.source.ColumnInfo;
import ydb.importer.TableDecision;

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
            final Type type = convertType(ci);
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
            case java.sql.Types.SMALLINT:
                return PrimitiveType.int16();
            case java.sql.Types.BOOLEAN:
                return PrimitiveType.bool();
            case java.sql.Types.INTEGER:
                return PrimitiveType.int32();
            case java.sql.Types.BIGINT:
                return PrimitiveType.int64();
            case java.sql.Types.DECIMAL:
            case java.sql.Types.NUMERIC:
                if (ci.getSqlScale() < 0)
                    return PrimitiveType.float64();
                if (ci.getSqlScale() == 0) {
                    if (ci.getSqlPrecision() == 0)
                        return PrimitiveType.float64();
                    if (ci.getSqlPrecision() < 10)
                        return PrimitiveType.int32();
                    if (ci.getSqlPrecision() < 20)
                        return PrimitiveType.int64();
                    // TEMP: only DECIMAL(22,9) is supported for table columns.
                    // For now Int64 is the best substitute for DECIMAL(38,0).
                    return PrimitiveType.int64();
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
                return PrimitiveType.utf8();
            case java.sql.Types.BINARY:
            case java.sql.Types.VARBINARY:
                return PrimitiveType.string();
            case java.sql.Types.BLOB:
            case java.sql.Types.LONGVARBINARY:
            case java.sql.Types.SQLXML:
                return PrimitiveType.int64(); // Id of record sequence in the separate table.
            case java.sql.Types.DATE:
                switch (tab.getOptions().getDateConv()) {
                    case DATE:
                        return PrimitiveType.date();
                    case INT:
                        return PrimitiveType.int32();
                    case STR:
                        return PrimitiveType.utf8();
                }
                return PrimitiveType.date();
            case java.sql.Types.TIMESTAMP:
                if (ci.getSqlScale()==0)
                    return PrimitiveType.datetime();
                return PrimitiveType.timestamp();
        }
        throw new IllegalArgumentException("Unsupported type code: " + ci.getSqlType());
    }

    private void addSyntheticKey(StringBuilder sb, Map<String, Type> types) {
        types.put(TargetTable.SYNTH_KEY_FIELD, PrimitiveType.string());
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
