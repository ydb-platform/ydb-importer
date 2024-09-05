package tech.ydb.importer.target;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.google.protobuf.ByteString;

import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.values.ListType;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.Value;

/**
 * JDBC to YDB BLOB copying logic. Each BLOB value is converted to a sequence of records in an
 * auxilary YDB table.
 *
 * @author zinal
 */
public class BlobSaver {
    public static final int BLOCK_SIZE = 65536;

    public static final StructType BLOB_ROW = StructType.of(
            "id", PrimitiveType.Int64,
            "pos", PrimitiveType.Int32,
            "val", PrimitiveType.Bytes
    );

    public static final ListType BLOB_LIST = ListType.of(BLOB_ROW);

    private final YdbUpsertOp upsertOp;
    private final AtomicLong nextIdGen = new AtomicLong(0);

    // max number of records before flush
    private final int maxBlobRecords;
    // target table value positions
    private final int posId;
    private final int posPos;
    private final int posVal;

    private final List<Value<?>> currentBulk = new ArrayList<>();


    public BlobSaver(String tablePath, SessionRetryContext ctx, ProgressCounter progress, int maxBlobRecords) {
        this.upsertOp = new YdbUpsertOp(
                ctx, tablePath, "blob rows upsert issue for " + tablePath, progress::addBlobRows
        );

        if (maxBlobRecords < 1) {
            maxBlobRecords = 1;
        } else if (maxBlobRecords > 1000) {
            maxBlobRecords = 1000;
        }
        this.maxBlobRecords = maxBlobRecords;
        this.posId = BLOB_ROW.getMemberIndex("id");
        this.posPos = BLOB_ROW.getMemberIndex("pos");
        this.posVal = BLOB_ROW.getMemberIndex("val");
    }


    public PrimitiveValue saveBlob(InputStream is) throws Exception {
        PrimitiveValue id = PrimitiveValue.newInt64(nextIdGen.incrementAndGet());

        byte[] block = new byte[BLOCK_SIZE];
        int position = 0;

        while (true) {
            // Read next BLOB block
            final int bytesRead = is.read(block);
            if (bytesRead < 1) {
                break;
            }

            // Create and append the block to the values list
            final Value<?>[] members = new Value<?>[3];
            members[posId] = id;
            members[posPos] = PrimitiveValue.newInt32(position);
            members[posVal] = PrimitiveValue.newBytes(ByteString.copyFrom(block, 0, bytesRead));
            currentBulk.add(BLOB_ROW.newValueUnsafe(members));
            position += 1;

            // Send the values list to YDB if it's time
            if (currentBulk.size() >= maxBlobRecords) {
                upsertOp.upload(BLOB_LIST.newValue(currentBulk));
                currentBulk.clear();
            }
        }

        return id;
    }

    public void flush() {
        if (!currentBulk.isEmpty()) {
            upsertOp.upload(BLOB_LIST.newValue(currentBulk));
            currentBulk.clear();
        }
    }
}
