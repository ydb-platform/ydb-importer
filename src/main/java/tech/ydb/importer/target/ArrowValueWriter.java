package tech.ydb.importer.target;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.nio.channels.Channels;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

import tech.ydb.table.query.arrow.ApacheArrowData;
import tech.ydb.table.query.arrow.ApacheArrowWriter;
import tech.ydb.table.values.DecimalType;
import tech.ydb.table.values.StructType;

/**
 * ValueWriter that stores produced values into an Apache Arrow row.
 */
public class ArrowValueWriter implements ValueWriter {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(ArrowValueWriter.class);

    private final String[] columnNames;
    private final DecimalType[] decimalTypes;
    private ApacheArrowWriter.Row row;

    public ArrowValueWriter(StructType type) {
        this.columnNames = new String[type.getMembersCount()];
        for (int i = 0; i < columnNames.length; i++) {
            columnNames[i] = type.getMemberName(i);
        }
        this.decimalTypes = ValueWriter.precomputeDecimalTypes(type);
    }

    public void setRow(ApacheArrowWriter.Row row) {
        this.row = row;
    }

    @Override
    public void writeNull(int idx) {
        row.writeNull(columnNames[idx]);
    }

    @Override
    public void writeBool(int idx, boolean v) {
        row.writeBool(columnNames[idx], v);
    }

    @Override
    public void writeInt32(int idx, int v) {
        row.writeInt32(columnNames[idx], v);
    }

    @Override
    public void writeUint32(int idx, long v) {
        row.writeUint32(columnNames[idx], v);
    }

    @Override
    public void writeInt64(int idx, long v) {
        row.writeInt64(columnNames[idx], v);
    }

    @Override
    public void writeUint64(int idx, long v) {
        row.writeUint64(columnNames[idx], v);
    }

    @Override
    public void writeFloat(int idx, float v) {
        row.writeFloat(columnNames[idx], v);
    }

    @Override
    public void writeDouble(int idx, double v) {
        row.writeDouble(columnNames[idx], v);
    }

    @Override
    public void writeText(int idx, String v) {
        row.writeText(columnNames[idx], v);
    }

    @Override
    public void writeBytes(int idx, byte[] v) {
        row.writeBytes(columnNames[idx], v);
    }

    @Override
    public void writeUuid(int idx, String v) {
        row.writeUuid(columnNames[idx], UUID.fromString(v));
    }

    @Override
    public void writeUuid(int idx, UUID v) {
        row.writeUuid(columnNames[idx], v);
    }

    @Override
    public void writeDate(int idx, LocalDate v) {
        row.writeDate(columnNames[idx], v);
    }

    @Override
    public void writeDate32(int idx, LocalDate v) {
        row.writeDate32(columnNames[idx], v);
    }

    @Override
    public void writeDatetime(int idx, Instant v) {
        row.writeDatetime(columnNames[idx], LocalDateTime.ofInstant(v, ZoneOffset.UTC));
    }

    @Override
    public void writeDatetime64(int idx, Instant v) {
        row.writeDatetime64(columnNames[idx], LocalDateTime.ofInstant(v, ZoneOffset.UTC));
    }

    @Override
    public void writeTimestamp(int idx, Instant v) {
        row.writeTimestamp(columnNames[idx], v);
    }

    @Override
    public void writeTimestamp64(int idx, Instant v) {
        row.writeTimestamp64(columnNames[idx], v);
    }

    @Override
    public void writeDecimal(int idx, BigDecimal v) {
        row.writeDecimal(columnNames[idx], decimalTypes[idx].newValue(v));
    }

    public static void logValues(ApacheArrowData data) {
        try (BufferAllocator alloc = new RootAllocator()) {
            Schema schema;
            try (ReadChannel ch = channelFrom(data.getSchema())) {
                schema = MessageSerializer.deserializeSchema(ch);
            }
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc);
                    ReadChannel ch = channelFrom(data.getData());
                    ArrowRecordBatch batch = MessageSerializer.deserializeRecordBatch(ch, alloc)) {
                new VectorLoader(root).load(batch);
                int rows = root.getRowCount();
                LOG.debug("********************************");
                LOG.debug("Problematic arrow batch dump START, size is {}", rows);
                for (int i = 0; i < rows; i++) {
                    final int rowIdx = i;
                    String content = root.getFieldVectors().stream()
                            .map(v -> v.getName() + "=" + v.getObject(rowIdx))
                            .collect(Collectors.joining(", "));
                    LOG.debug("{} {}", i, content);
                }
                LOG.debug("Problematic arrow batch dump FINISH");
                LOG.debug("********************************");
            }
        } catch (Exception e) {
            LOG.debug("Failed to decode arrow batch for dump", e);
        }
    }

    private static ReadChannel channelFrom(ByteString bytes) {
        return new ReadChannel(Channels.newChannel(new ByteArrayInputStream(bytes.toByteArray())));
    }
}
