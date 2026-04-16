package tech.ydb.importer.target;

import java.io.Reader;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.values.ListType;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.Value;

/**
 * JDBC to YDB CLOB copying logic. Each CLOB value is converted to a sequence of text records
 * in an auxiliary YDB table.
 */
public class ClobReader extends ValueReader {

    public static final int BLOCK_SIZE = 32768;

    public static final StructType CLOB_ROW = StructType.of(
            "id", PrimitiveType.Int64,
            "pos", PrimitiveType.Int32,
            "val", PrimitiveType.Text
    );

    public static final ListType CLOB_LIST = ListType.of(CLOB_ROW);

    private final YdbUpsertOp upsertOp;
    private long nextClobId;

    private final int maxClobRecords;
    private final int posId;
    private final int posPos;
    private final int posVal;

    private final List<Value<?>> currentBulk = new ArrayList<>();

    public ClobReader(String tablePath, SessionRetryContext ctx,
                      ProgressCounter progress, int maxClobRecords) {
        this.upsertOp = new YdbUpsertOp(
                ctx, tablePath, "clob rows upsert issue for " + tablePath, progress::countBlobRows
        );

        if (maxClobRecords < 1) {
            maxClobRecords = 1;
        } else if (maxClobRecords > 1000) {
            maxClobRecords = 1000;
        }
        this.maxClobRecords = maxClobRecords;
        this.posId = CLOB_ROW.getMemberIndex("id");
        this.posPos = CLOB_ROW.getMemberIndex("pos");
        this.posVal = CLOB_ROW.getMemberIndex("val");
    }

    public void setNextClobId(long id) {
        this.nextClobId = id;
    }

    @Override
    public void read(ResultSet rs, int rsIndex, int targetIndex,
                     ValueWriter writer, SynthKey synthKey) throws Exception {
        Reader reader = rs.getCharacterStream(rsIndex);
        if (rs.wasNull()) {
            if (reader != null) {
                reader.close();
            }
            writer.writeNull(targetIndex);
            if (synthKey != null) {
                synthKey.hashNull();
            }
            return;
        }
        try (Reader r = reader) {
            long id = nextClobId;
            saveClob(PrimitiveValue.newInt64(id), r);
            writer.writeInt64(targetIndex, id);
            if (synthKey != null) {
                synthKey.hashLong(id);
            }
        }
    }

    private void saveClob(PrimitiveValue id, Reader reader) throws Exception {
        char[] block = new char[BLOCK_SIZE];
        int position = 0;

        while (true) {
            final int charsRead = reader.read(block);
            if (charsRead < 1) {
                break;
            }

            final Value<?>[] members = new Value<?>[3];
            members[posId] = id;
            members[posPos] = PrimitiveValue.newInt32(position);
            members[posVal] = PrimitiveValue.newText(new String(block, 0, charsRead));
            currentBulk.add(CLOB_ROW.newValueUnsafe(members));
            position += 1;

            if (currentBulk.size() >= maxClobRecords) {
                upsertOp.upload(CLOB_LIST.newValue(currentBulk));
                currentBulk.clear();
            }
        }
    }

    @Override
    public void flush() {
        if (!currentBulk.isEmpty()) {
            upsertOp.upload(CLOB_LIST.newValue(currentBulk));
            currentBulk.clear();
        }
    }
}
