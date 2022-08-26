package ydb.importer.target;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.google.protobuf.ByteString;
import com.yandex.ydb.table.values.ListType;
import com.yandex.ydb.table.values.PrimitiveType;
import com.yandex.ydb.table.values.PrimitiveValue;
import com.yandex.ydb.table.values.StructType;
import com.yandex.ydb.table.values.Value;

/**
 * JDBC to YDB BLOB copying logic.
 * Each BLOB value is converted to a sequence of records in an auxilary YDB table.
 * @author zinal
 */
public class BlobSaver {

    public final static int BLOCK_SIZE = 65536;

    public final static StructType BLOB_ROW = StructType.of(
                        "id", PrimitiveType.int64(),
                        "pos", PrimitiveType.int32(),
                        "val", PrimitiveType.string() );
    public final static ListType BLOB_LIST = ListType.of(BLOB_ROW);

    // Id generator for the current worker thread.
    private final static ThreadLocal<Long> VALUE_ID = ThreadLocal.withInitial(() -> -1L);
    
    private final ProgressCounter progress;
    // BLOB path -> unwritten BLOB records
    private final Map<String, Datum> data = new HashMap<>();
    // max number of records before flush
    private final int maxBlobRecords;
    // target table value positions
    private final int posId;
    private final int posPos;
    private final int posVal;

    public BlobSaver(int maxBlobRecords, ProgressCounter progress) {
        this.progress = progress;
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

    public static void initCounter(int counter) {
        final long base = ( ((long) counter) )
                * (((long)Integer.MAX_VALUE) + 1L)
                * 256L;
        VALUE_ID.set(base);
    }

    private Datum makeDatum(String blobPath) {
        Datum v = data.get(blobPath);
        if (v==null) {
            v = new Datum(progress, blobPath);
            data.put(blobPath, v);
        }
        return v;
    }

    public long nextId() {
        final long x = VALUE_ID.get();
        if (x < 0L)
            throw new IllegalStateException("Worker thread has not been initialized");
        final long v = x + 1;
        VALUE_ID.set(v);
        return v;
    }

    public long saveBlob(YdbUpsertOp ydbOp, InputStream is, String blobPath) throws Exception {
        final long id = nextId();
        final Value<?> idValue = PrimitiveValue.int64(id);
        final Datum v = makeDatum(blobPath);
        int position = 0;
        final byte[] block = new byte[BLOCK_SIZE];
        while (true) {
            // Read next BLOB block
            final int bytesRead = is.read(block);
            if (bytesRead < 1)
                break;
            // Create and append the block to the values list
            final Value<?> members[] = new Value<?>[3];
            members[posId] = idValue;
            members[posPos] = PrimitiveValue.int32(position);
            members[posVal] = PrimitiveValue.string(ByteString.copyFrom(block, 0, bytesRead));
            v.values.add(BLOB_ROW.newValueUnsafe(members));
            ++position;
            // Send the values list to YDB if it's time
            if (v.values.size() >= maxBlobRecords) {
                ydbOp.start(blobPath, BLOB_LIST.newValue(v.values), v.counter);
                v.values.clear();
            }
        }
        return id;
    }

    public void flush(YdbUpsertOp ydbOp) {
        for (Datum d : data.values()) {
            if (! d.values.isEmpty())
                ydbOp.start(d.tablePath, BLOB_LIST.newValue(d.values), d.counter);
        }
        data.clear();
        ydbOp.finish();
    }

    public static final class Datum {
        public final String tablePath;
        public final AnyCounter counter;
        public final List<Value> values;

        public Datum(ProgressCounter pc, String tablePath) {
            this.tablePath = tablePath;
            this.counter = new BlobCounter("blob rows upsert issue for " + tablePath, pc);
            this.values = new ArrayList<>();
        }
    }

}
