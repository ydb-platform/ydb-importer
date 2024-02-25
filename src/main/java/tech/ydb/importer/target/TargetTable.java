package tech.ydb.importer.target;

import tech.ydb.importer.TableDecision;
import tech.ydb.table.values.StructType;

/**
 * Structure of the target YDB table.
 *
 * @author zinal
 */
public class TargetTable extends tech.ydb.importer.config.JdomHelper {

    public static final String SYNTH_KEY_FIELD = "ydb_synth_key";

    private final TableDecision original;
    private final String fullName;
    private final String yqlScript;
    private StructType fields;
    private int synthKeyPos;

    public TargetTable(TableDecision original, String fullName,
            String yqlScript, StructType fields) {
        this.original = original;
        this.fullName = fullName;
        this.yqlScript = yqlScript;
        this.fields = fields;
        findSynthKey();
    }

    public boolean isValid() {
        if (original == null) {
            return false;
        }
        if (isBlank(fullName) || isBlank(yqlScript)) {
            return false;
        }
        if (fields == null || fields.getMembersCount() == 0) {
            return false;
        }
        return true;
    }

    public TableDecision getOriginal() {
        return original;
    }

    public String getFullName() {
        return fullName;
    }

    public String getYqlScript() {
        return yqlScript;
    }

    public StructType getFields() {
        return fields;
    }

    public void setFields(StructType fields) {
        this.fields = fields;
        findSynthKey();
    }

    public int getSynthKeyPos() {
        return synthKeyPos;
    }

    public void setSynthKeyPos(int synthKeyPos) {
        this.synthKeyPos = synthKeyPos;
    }

    public boolean hasSynthKey() {
        return synthKeyPos >= 0;
    }

    private void findSynthKey() {
        synthKeyPos = -1;
        if (fields == null) {
            return;
        }
        for (int i = 0; i < fields.getMembersCount(); ++i) {
            if (SYNTH_KEY_FIELD.equals(fields.getMemberName(i))) {
                synthKeyPos = i;
                break;
            }
        }
    }

}
