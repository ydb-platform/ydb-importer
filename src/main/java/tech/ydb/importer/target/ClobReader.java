package tech.ydb.importer.target;

import java.io.Reader;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.query.BulkUpsertData;
import tech.ydb.table.values.ListType;
import tech.ydb.table.values.ListValue;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.Value;

/**
 * JDBC to YDB CLOB copying logic. Each CLOB value is converted to a sequence of Utf8 chunks in an
 * auxiliary YDB table.
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
    private long nextClobId = 0;

    private final int maxClobRecords;
    private final int posId;
    private final int posPos;
    private final int posVal;

    private final List<Value<?>> currentBulk = new ArrayList<>();

    public ClobReader(String tablePath, SessionRetryContext ctx, ProgressCounter progress, int maxClobRecords) {
        this.upsertOp = new YdbUpsertOp(
                ctx, tablePath, "clob rows upsert issue for " + tablePath, progress::countBlobRows
        );
        this.maxClobRecords = maxClobRecords;
        this.posId = CLOB_ROW.getMemberIndex("id");
        this.posPos = CLOB_ROW.getMemberIndex("pos");
        this.posVal = CLOB_ROW.getMemberIndex("val");
    }

    public void setNextClobId(long id) {
        this.nextClobId = id;
    }

    @Override
    public void read(ResultSet rs, int rsIdx, int targetIdx, ValueWriter writer, SynthKey synthKey) throws Exception {
        try (Reader r = rs.getCharacterStream(rsIdx)) {
            if (rs.wasNull()) {
                if (synthKey != null) {
                    synthKey.hashNull();
                }
                writer.writeNull(targetIdx);
                return;
            }

            long id = nextClobId;
            saveClob(PrimitiveValue.newInt64(id), r);
            if (synthKey != null) {
                synthKey.hashLong(id);
            }
            writer.writeInt64(targetIdx, id);
        }
    }

    public void saveClob(PrimitiveValue id, Reader r) throws Exception {
        char[] block = new char[BLOCK_SIZE];
        int position = 0;
        int pending = -1;

        while (true) {
            int offset = 0;
            if (pending >= 0) {
                block[0] = (char) pending;
                pending = -1;
                offset = 1;
            }

            final int charsRead = r.read(block, offset, block.length - offset);
            int len;
            if (charsRead < 0) {
                len = offset;
            } else {
                len = offset + charsRead;
                if (Character.isHighSurrogate(block[len - 1])) {
                    pending = block[len - 1];
                    len--;
                }
            }

            if (len > 0) {
                final Value<?>[] members = new Value<?>[3];
                members[posId] = id;
                members[posPos] = PrimitiveValue.newInt32(position);
                members[posVal] = PrimitiveValue.newText(new String(block, 0, len));
                currentBulk.add(CLOB_ROW.newValueUnsafe(members));
                position += 1;

                if (currentBulk.size() >= maxClobRecords) {
                    final ListValue lv = CLOB_LIST.newValue(currentBulk);
                    upsertOp.upload(new BulkUpsertData(lv), currentBulk.size(),
                            () -> RowValueWriter.logValues(lv));
                    currentBulk.clear();
                }
            }

            if (charsRead < 0) {
                break;
            }
        }
    }

    @Override
    public void flush() {
        if (!currentBulk.isEmpty()) {
            final ListValue lv = CLOB_LIST.newValue(currentBulk);
            upsertOp.upload(new BulkUpsertData(lv), currentBulk.size(),
                    () -> RowValueWriter.logValues(lv));
            currentBulk.clear();
        }
    }
}
