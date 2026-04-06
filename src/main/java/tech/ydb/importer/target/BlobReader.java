package tech.ydb.importer.target;

import java.io.InputStream;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

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
public class BlobReader extends ValueReader {
    public static final int BLOCK_SIZE = 65536;

    public static final StructType BLOB_ROW = StructType.of(
            "id", PrimitiveType.Int64,
            "pos", PrimitiveType.Int32,
            "val", PrimitiveType.Bytes
    );

    public static final ListType BLOB_LIST = ListType.of(BLOB_ROW);

    private static final int ROW_BITS = 40;
    private static final int PARTITION_BITS = 23;
    private static final long MAX_ROW_INDEX = 1L << ROW_BITS;
    private static final int MAX_PARTITION_IDX = 1 << PARTITION_BITS;

    private final YdbUpsertOp upsertOp;
    private long nextBlobId;

    // max number of records before flush
    private final int maxBlobRecords;
    // target table value positions
    private final int posId;
    private final int posPos;
    private final int posVal;

    private final boolean isBlob;
    private final List<Value<?>> currentBulk = new ArrayList<>();

    public BlobReader(String tablePath, SessionRetryContext ctx, ProgressCounter progress, int maxBlobRecords,
            boolean isBlob) {
        this.upsertOp = new YdbUpsertOp(
                ctx, tablePath, "blob rows upsert issue for " + tablePath, progress::countBlobRows
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
        this.isBlob = isBlob;
    }

    /** Sets the blob id to use for the next call to {@link #read}. */
    public void setNextBlobId(long id) {
        this.nextBlobId = id;
    }

    /** Packs (partitionIdx, rowIndex) into a single Int64 blob id. */
    public static long packBlobId(int partitionIdx, long rowIndex) {
        if (partitionIdx < 0 || partitionIdx >= MAX_PARTITION_IDX) {
            throw new IllegalArgumentException("partitionIdx out of range: " + partitionIdx);
        }
        if (rowIndex < 0 || rowIndex >= MAX_ROW_INDEX) {
            throw new IllegalArgumentException("rowIndex out of range: " + rowIndex);
        }
        return ((long) partitionIdx << ROW_BITS) | rowIndex;
    }

    @Override
    public void read(ResultSet rs, int rsIndex, int targetIndex,
                     ValueWriter writer, SynthKey synthKey) throws Exception {
        try (InputStream is = openStream(rs, rsIndex)) {
            if (rs.wasNull()) {
                writer.writeNull(targetIndex);
                if (synthKey != null) {
                    synthKey.hashNull();
                }
                return;
            }

            long id = nextBlobId;
            PrimitiveValue ydbId = PrimitiveValue.newInt64(id);
            saveBlob(ydbId, is);
            writer.writeInt64(targetIndex, id);
            if (synthKey != null) {
                synthKey.hashLong(id);
            }
        }
    }

    private InputStream openStream(ResultSet rs, int index) throws Exception {
        if (isBlob) {
            java.sql.Blob blob = rs.getBlob(index);
            return blob == null ? null : blob.getBinaryStream();
        }
        return rs.getBinaryStream(index);
    }

    public void saveBlob(PrimitiveValue id, InputStream is) throws Exception {
        byte[] block = new byte[BLOCK_SIZE];
        int position = 0;

        while (true) {
            final int bytesRead = is.read(block);
            if (bytesRead < 1) {
                break;
            }

            final Value<?>[] members = new Value<?>[3];
            members[posId] = id;
            members[posPos] = PrimitiveValue.newInt32(position);
            members[posVal] = PrimitiveValue.newBytes(ByteString.copyFrom(block, 0, bytesRead));
            currentBulk.add(BLOB_ROW.newValueUnsafe(members));
            position += 1;

            if (currentBulk.size() >= maxBlobRecords) {
                upsertOp.upload(BLOB_LIST.newValue(currentBulk));
                currentBulk.clear();
            }
        }
    }

    @Override
    public void flush() {
        if (!currentBulk.isEmpty()) {
            upsertOp.upload(BLOB_LIST.newValue(currentBulk));
            currentBulk.clear();
        }
    }
}
