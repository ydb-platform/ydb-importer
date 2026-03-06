package tech.ydb.importer.target;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.importer.TableDecision;
import tech.ydb.importer.source.ColumnInfo;
import tech.ydb.table.query.arrow.ApacheArrowData;
import tech.ydb.table.query.arrow.ApacheArrowWriter;
import tech.ydb.table.values.DecimalType;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.Type;

/**
 * Builds Apache Arrow columnar batches from JDBC ResultSet rows.
 * Type mapping mirrors {@link ValueReader} to ensure
 * identical conversion semantics between row-based and Arrow paths.
 */
public class ArrowBatchBuilder implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ArrowBatchBuilder.class);

    private final StructType paramType;
    private final int maxBatchRows;
    private final BufferAllocator allocator;
    private final ApacheArrowWriter writer;
    private final List<ColumnMapping> mappings;
    private final List<BlobMapping> blobMappings;
    private final SynthKey synthKey;
    private final String synthKeyCol;

    public ArrowBatchBuilder(StructType paramType, ResultSetMetaData rsmd,
                             TableDecision tab, int maxBatchRows,
                             Map<String, BlobAccumulator> blobAccumulators) throws Exception {
        this.paramType = paramType;
        this.maxBatchRows = maxBatchRows;
        this.allocator = new RootAllocator();
        this.mappings = buildColumnMappings(paramType, rsmd, tab);
        this.blobMappings = buildBlobMappings(paramType, rsmd, tab, blobAccumulators);

        if (tab.getTarget().hasSynthKey()) {
            this.synthKey = new SynthKey();
            this.synthKeyCol = TargetTable.SYNTH_KEY_FIELD;
        } else {
            this.synthKey = null;
            this.synthKeyCol = null;
        }

        ApacheArrowWriter.Schema schema = ApacheArrowWriter.newSchema();
        for (int i = 0; i < paramType.getMembersCount(); i++) {
            schema.addColumn(paramType.getMemberName(i), paramType.getMemberType(i));
        }
        this.writer = schema.createWriter(allocator);
    }

    public ApacheArrowWriter.Batch createBatch() {
        return writer.createNewBatch(maxBatchRows);
    }

    public void writeRow(ResultSet rs, ApacheArrowWriter.Batch batch) throws Exception {
        ApacheArrowWriter.Row row = batch.writeNextRow();
        for (ColumnMapping m : mappings) {
            String colName = paramType.getMemberName(m.structIndex);
            Type ydbType = unwrapOptional(paramType.getMemberType(m.structIndex));

            if (ydbType.getKind() == Type.Kind.PRIMITIVE) {
                writePrimitiveValue(rs, m.rsIndex, row, colName,
                        (PrimitiveType) ydbType, m.sqlType, synthKey);
            } else if (ydbType.getKind() == Type.Kind.DECIMAL) {
                BigDecimal val = rs.getBigDecimal(m.rsIndex);
                if (rs.wasNull()) {
                    row.writeNull(colName);
                    if (synthKey != null) {
                        synthKey.updateSeparator();
                    }
                } else {
                    DecimalType dt = (DecimalType) ydbType;
                    row.writeDecimal(colName, dt.newValue(val));
                    if (synthKey != null) {
                        byte[] bytes = val.toBigInteger().toByteArray();
                        ByteBuffer buf = ByteBuffer.allocate(bytes.length + Integer.BYTES);
                        buf.putInt(val.scale());
                        buf.put(bytes);
                        synthKey.update(buf);
                        synthKey.updateSeparator();
                    }
                }
            } else {
                String val = rs.getString(m.rsIndex);
                if (rs.wasNull()) {
                    row.writeNull(colName);
                    if (synthKey != null) {
                        synthKey.updateSeparator();
                    }
                } else {
                    row.writeText(colName, val);
                    if (synthKey != null) {
                        synthKey.update(val.getBytes());
                        synthKey.updateSeparator();
                    }
                }
            }
        }
        for (BlobMapping bm : blobMappings) {
            String colName = paramType.getMemberName(bm.structIndex);
            long id = bm.accumulator.writeBlob(synthKey, rs, bm.rsIndex);
            if (id < 0) {
                row.writeNull(colName);
            } else {
                row.writeInt64(colName, id);
            }
        }
        if (synthKey != null) {
            row.writeText(synthKeyCol, synthKey.buildString());
        }
    }

    public ApacheArrowData buildBatch(ApacheArrowWriter.Batch batch) throws Exception {
        return batch.buildBatch();
    }

    public void flush() throws Exception {
        for (BlobMapping bm : blobMappings) {
            bm.accumulator.flush();
        }
    }

    @Override
    public void close() {
        for (BlobMapping bm : blobMappings) {
            bm.accumulator.close();
        }
        try {
            writer.close();
        } catch (Exception e) {
            LOG.debug("Failed to close Arrow writer", e);
        }
        allocator.close();
    }

    private static void writePrimitiveValue(ResultSet rs, int rsIndex,
                                            ApacheArrowWriter.Row row,
                                            String colName, PrimitiveType type, int sqlType,
                                            SynthKey synthKey) throws Exception {
        switch (type) {
            case Bool:
                writeBoolValue(rs, rsIndex, row, colName, sqlType, synthKey);
                break;
            case Int32:
                writeInt32Value(rs, rsIndex, row, colName, sqlType, synthKey);
                break;
            case Int64:
                writeInt64Value(rs, rsIndex, row, colName, sqlType, synthKey);
                break;
            case Uint32:
                writeUint32Value(rs, rsIndex, row, colName, sqlType, synthKey);
                break;
            case Uint64:
                writeUint64Value(rs, rsIndex, row, colName, sqlType, synthKey);
                break;
            case Text:
                writeTextValue(rs, rsIndex, row, colName, sqlType, synthKey);
                break;
            case Date:
                writeDateValue(rs, rsIndex, row, colName, sqlType, false, synthKey);
                break;
            case Date32:
                writeDateValue(rs, rsIndex, row, colName, sqlType, true, synthKey);
                break;
            case Datetime:
            case Timestamp:
            case Datetime64:
            case Timestamp64:
                writeTimestampType(rs, rsIndex, row, colName, type, synthKey);
                break;
            case Float:
                writeFloatValue(rs, rsIndex, row, colName, synthKey);
                break;
            case Double:
                writeDoubleValue(rs, rsIndex, row, colName, synthKey);
                break;
            case Bytes:
                writeBytesValue(rs, rsIndex, row, colName, synthKey);
                break;
            case Uuid:
                writeUuidValue(rs, rsIndex, row, colName, sqlType, synthKey);
                break;
            case Int8:
                writeInt8Value(rs, rsIndex, row, colName, synthKey);
                break;
            case Int16:
                writeInt16Value(rs, rsIndex, row, colName, synthKey);
                break;
            case Uint8:
            case Uint16:
                writeUint8or16Value(rs, rsIndex, row, colName, type, synthKey);
                break;
            default:
                throw new IllegalStateException(
                        "Unsupported Arrow type " + type + " for column " + colName);
        }
    }


    private static void writeBoolValue(ResultSet rs, int rsIndex,
                                       ApacheArrowWriter.Row row, String colName,
                                       int sqlType, SynthKey synthKey) throws Exception {
        switch (sqlType) {
            case java.sql.Types.SMALLINT:
            case java.sql.Types.INTEGER:
            case java.sql.Types.BIGINT:
            case java.sql.Types.DECIMAL:
            case java.sql.Types.NUMERIC:
            case java.sql.Types.FLOAT:
            case java.sql.Types.DOUBLE: {
                int v = rs.getInt(rsIndex);
                if (rs.wasNull()) {
                    writeNull(row, colName, synthKey);
                } else {
                    row.writeBool(colName, v != 0);
                    if (synthKey != null) {
                        ByteBuffer buf = ByteBuffer.allocate(1);
                        buf.put((byte) (v != 0 ? 1 : 0));
                        synthKey.update(buf);
                        synthKey.updateSeparator();
                    }
                }
                break;
            }
            case java.sql.Types.CHAR:
            case java.sql.Types.NCHAR:
            case java.sql.Types.VARCHAR:
            case java.sql.Types.NVARCHAR: {
                String v = rs.getString(rsIndex);
                if (rs.wasNull()) {
                    writeNull(row, colName, synthKey);
                } else {
                    row.writeBool(colName, str2bool(v));
                    if (synthKey != null) {
                        synthKey.update(v.getBytes());
                        synthKey.updateSeparator();
                    }
                }
                break;
            }
            default: {
                boolean v = rs.getBoolean(rsIndex);
                if (rs.wasNull()) {
                    writeNull(row, colName, synthKey);
                } else {
                    row.writeBool(colName, v);
                    if (synthKey != null) {
                        ByteBuffer buf = ByteBuffer.allocate(1);
                        buf.put((byte) (v ? 1 : 0));
                        synthKey.update(buf);
                        synthKey.updateSeparator();
                    }
                }
                break;
            }
        }
    }

    private static void writeInt32Value(ResultSet rs, int rsIndex,
                                        ApacheArrowWriter.Row row, String colName,
                                        int sqlType, SynthKey synthKey) throws Exception {
        switch (sqlType) {
            case java.sql.Types.TIME: {
                java.sql.Time v = rs.getTime(rsIndex);
                if (rs.wasNull()) {
                    writeNull(row, colName, synthKey);
                } else {
                    writeInt32(row, colName, time2int(v), synthKey);
                }
                break;
            }
            case java.sql.Types.DATE: {
                java.sql.Date v = rs.getDate(rsIndex);
                if (rs.wasNull()) {
                    writeNull(row, colName, synthKey);
                } else {
                    writeInt32(row, colName, date2int(v), synthKey);
                }
                break;
            }
            default: {
                int v = rs.getInt(rsIndex);
                if (rs.wasNull()) {
                    writeNull(row, colName, synthKey);
                } else {
                    writeInt32(row, colName, v, synthKey);
                }
                break;
            }
        }
    }

    private static void writeInt64Value(ResultSet rs, int rsIndex,
                                        ApacheArrowWriter.Row row, String colName,
                                        int sqlType, SynthKey synthKey) throws Exception {
        switch (sqlType) {
            case java.sql.Types.DATE: {
                java.sql.Date v = rs.getDate(rsIndex);
                if (rs.wasNull()) {
                    writeNull(row, colName, synthKey);
                } else {
                    writeLong(row, colName, date2int(v), PrimitiveType.Int64, synthKey);
                }
                break;
            }
            case java.sql.Types.TIMESTAMP: {
                Timestamp v = rs.getTimestamp(rsIndex);
                if (rs.wasNull()) {
                    writeNull(row, colName, synthKey);
                } else {
                    writeLong(row, colName, v.getTime(), PrimitiveType.Int64, synthKey);
                }
                break;
            }
            default: {
                long v = rs.getLong(rsIndex);
                if (rs.wasNull()) {
                    writeNull(row, colName, synthKey);
                } else {
                    writeLong(row, colName, v, PrimitiveType.Int64, synthKey);
                }
                break;
            }
        }
    }

    private static void writeUint32Value(ResultSet rs, int rsIndex,
                                         ApacheArrowWriter.Row row, String colName,
                                         int sqlType, SynthKey synthKey) throws Exception {
        if (sqlType == java.sql.Types.DATE) {
            java.sql.Date v = rs.getDate(rsIndex);
            if (rs.wasNull()) {
                writeNull(row, colName, synthKey);
            } else {
                writeLong(row, colName, date2int(v), PrimitiveType.Uint32, synthKey);
            }
        } else {
            long v = rs.getLong(rsIndex);
            if (rs.wasNull()) {
                writeNull(row, colName, synthKey);
            } else {
                writeLong(row, colName, v, PrimitiveType.Uint32, synthKey);
            }
        }
    }

    private static void writeUint64Value(ResultSet rs, int rsIndex,
                                         ApacheArrowWriter.Row row, String colName,
                                         int sqlType, SynthKey synthKey) throws Exception {
        switch (sqlType) {
            case java.sql.Types.DATE: {
                java.sql.Date v = rs.getDate(rsIndex);
                if (rs.wasNull()) {
                    writeNull(row, colName, synthKey);
                } else {
                    writeLong(row, colName, date2int(v), PrimitiveType.Uint64, synthKey);
                }
                break;
            }
            case java.sql.Types.TIMESTAMP: {
                Timestamp v = rs.getTimestamp(rsIndex);
                if (rs.wasNull()) {
                    writeNull(row, colName, synthKey);
                } else {
                    writeLong(row, colName, v.getTime(), PrimitiveType.Uint64, synthKey);
                }
                break;
            }
            default: {
                long v = rs.getLong(rsIndex);
                if (rs.wasNull()) {
                    writeNull(row, colName, synthKey);
                } else {
                    writeLong(row, colName, v, PrimitiveType.Uint64, synthKey);
                }
                break;
            }
        }
    }

    private static void writeTextValue(ResultSet rs, int rsIndex,
                                       ApacheArrowWriter.Row row, String colName,
                                       int sqlType, SynthKey synthKey) throws Exception {
        switch (sqlType) {
            case java.sql.Types.DATE: {
                java.sql.Date v = rs.getDate(rsIndex);
                if (rs.wasNull()) {
                    writeNull(row, colName, synthKey);
                } else {
                    writeString(row, colName, date2str(v), synthKey);
                }
                break;
            }
            case java.sql.Types.TIMESTAMP: {
                Timestamp v = rs.getTimestamp(rsIndex);
                if (rs.wasNull()) {
                    writeNull(row, colName, synthKey);
                } else {
                    writeString(row, colName, v.toString(), synthKey);
                }
                break;
            }
            default: {
                String v = rs.getString(rsIndex);
                if (rs.wasNull()) {
                    writeNull(row, colName, synthKey);
                } else {
                    writeString(row, colName, v, synthKey);
                }
                break;
            }
        }
    }

    private static void writeDateValue(ResultSet rs, int rsIndex,
                                       ApacheArrowWriter.Row row, String colName,
                                       int sqlType, boolean date32,
                                       SynthKey synthKey) throws Exception {
        if (sqlType == java.sql.Types.TIMESTAMP) {
            Timestamp v = rs.getTimestamp(rsIndex);
            if (rs.wasNull()) {
                writeNull(row, colName, synthKey);
            } else {
                writeDateFromTs(row, colName, v, date32, synthKey);
            }
        } else {
            java.sql.Date v = rs.getDate(rsIndex);
            if (rs.wasNull()) {
                writeNull(row, colName, synthKey);
            } else {
                writeDateVal(row, colName, v, date32, synthKey);
            }
        }
    }

    private static void writeTimestampType(ResultSet rs, int rsIndex,
                                           ApacheArrowWriter.Row row, String colName,
                                           PrimitiveType type,
                                           SynthKey synthKey) throws Exception {
        Timestamp v = rs.getTimestamp(rsIndex);
        if (rs.wasNull()) {
            writeNull(row, colName, synthKey);
        } else {
            writeTimestampVal(row, colName, v, type, synthKey);
        }
    }

    private static void writeFloatValue(ResultSet rs, int rsIndex,
                                        ApacheArrowWriter.Row row, String colName,
                                        SynthKey synthKey) throws Exception {
        float v = rs.getFloat(rsIndex);
        if (rs.wasNull()) {
            writeNull(row, colName, synthKey);
        } else {
            row.writeFloat(colName, v);
            if (synthKey != null) {
                ByteBuffer buf = ByteBuffer.allocate(Float.BYTES);
                buf.putFloat(v);
                synthKey.update(buf);
                synthKey.updateSeparator();
            }
        }
    }

    private static void writeDoubleValue(ResultSet rs, int rsIndex,
                                         ApacheArrowWriter.Row row, String colName,
                                         SynthKey synthKey) throws Exception {
        double v = rs.getDouble(rsIndex);
        if (rs.wasNull()) {
            writeNull(row, colName, synthKey);
        } else {
            row.writeDouble(colName, v);
            if (synthKey != null) {
                ByteBuffer buf = ByteBuffer.allocate(Double.BYTES);
                buf.putDouble(v);
                synthKey.update(buf);
                synthKey.updateSeparator();
            }
        }
    }

    private static void writeBytesValue(ResultSet rs, int rsIndex,
                                        ApacheArrowWriter.Row row, String colName,
                                        SynthKey synthKey) throws Exception {
        byte[] v = rs.getBytes(rsIndex);
        if (rs.wasNull()) {
            writeNull(row, colName, synthKey);
        } else {
            row.writeBytes(colName, v);
            if (synthKey != null) {
                synthKey.update(v);
                synthKey.updateSeparator();
            }
        }
    }

    private static void writeUuidValue(ResultSet rs, int rsIndex,
                                       ApacheArrowWriter.Row row, String colName,
                                       int sqlType, SynthKey synthKey) throws Exception {
        switch (sqlType) {
            case java.sql.Types.BINARY:
            case java.sql.Types.VARBINARY: {
                byte[] v = rs.getBytes(rsIndex);
                if (rs.wasNull()) {
                    writeNull(row, colName, synthKey);
                } else {
                    ByteBuffer bb = ByteBuffer.wrap(v);
                    row.writeUuid(colName, new UUID(bb.getLong(), bb.getLong()));
                    if (synthKey != null) {
                        synthKey.update(v);
                        synthKey.updateSeparator();
                    }
                }
                break;
            }
            default: {
                String v = rs.getString(rsIndex);
                if (rs.wasNull()) {
                    writeNull(row, colName, synthKey);
                } else {
                    row.writeUuid(colName, UUID.fromString(v));
                    if (synthKey != null) {
                        synthKey.update(v.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                        synthKey.updateSeparator();
                    }
                }
                break;
            }
        }
    }

    private static void writeInt8Value(ResultSet rs, int rsIndex,
                                       ApacheArrowWriter.Row row, String colName,
                                       SynthKey synthKey) throws Exception {
        byte v = rs.getByte(rsIndex);
        if (rs.wasNull()) {
            writeNull(row, colName, synthKey);
        } else {
            row.writeInt8(colName, v);
            if (synthKey != null) {
                ByteBuffer buf = ByteBuffer.allocate(1);
                buf.put(v);
                synthKey.update(buf);
                synthKey.updateSeparator();
            }
        }
    }

    private static void writeInt16Value(ResultSet rs, int rsIndex,
                                        ApacheArrowWriter.Row row, String colName,
                                        SynthKey synthKey) throws Exception {
        short v = rs.getShort(rsIndex);
        if (rs.wasNull()) {
            writeNull(row, colName, synthKey);
        } else {
            row.writeInt16(colName, v);
            if (synthKey != null) {
                ByteBuffer buf = ByteBuffer.allocate(Short.BYTES);
                buf.putShort(v);
                synthKey.update(buf);
                synthKey.updateSeparator();
            }
        }
    }

    private static void writeUint8or16Value(ResultSet rs, int rsIndex,
                                            ApacheArrowWriter.Row row, String colName,
                                            PrimitiveType type,
                                            SynthKey synthKey) throws Exception {
        int v = rs.getInt(rsIndex);
        if (rs.wasNull()) {
            writeNull(row, colName, synthKey);
        } else {
            if (type == PrimitiveType.Uint8) {
                row.writeUint8(colName, v);
            } else {
                row.writeUint16(colName, v);
            }
            if (synthKey != null) {
                ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES);
                buf.putInt(v);
                synthKey.update(buf);
                synthKey.updateSeparator();
            }
        }
    }


    private static void writeNull(ApacheArrowWriter.Row row, String col, SynthKey sk) {
        row.writeNull(col);
        if (sk != null) {
            sk.updateSeparator();
        }
    }

    private static void writeInt32(ApacheArrowWriter.Row row, String col, int v, SynthKey sk) {
        row.writeInt32(col, v);
        if (sk != null) {
            ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES);
            buf.putInt(v);
            sk.update(buf);
            sk.updateSeparator();
        }
    }

    private static void writeLong(ApacheArrowWriter.Row row, String col, long v,
                                  PrimitiveType type, SynthKey sk) {
        if (type == PrimitiveType.Uint32) {
            row.writeUint32(col, v);
        } else if (type == PrimitiveType.Int64) {
            row.writeInt64(col, v);
        } else {
            row.writeUint64(col, v);
        }
        if (sk != null) {
            ByteBuffer buf = ByteBuffer.allocate(Long.BYTES);
            buf.putLong(v);
            sk.update(buf);
            sk.updateSeparator();
        }
    }

    private static void writeString(ApacheArrowWriter.Row row, String col, String v, SynthKey sk) {
        row.writeText(col, v);
        if (sk != null) {
            sk.update(v.getBytes());
            sk.updateSeparator();
        }
    }

    private static void writeDateVal(ApacheArrowWriter.Row row, String col, java.sql.Date v,
                                     boolean date32, SynthKey sk) {
        if (date32) {
            row.writeDate32(col, v.toLocalDate());
        } else {
            row.writeDate(col, v.toLocalDate());
        }
        if (sk != null) {
            ByteBuffer buf = ByteBuffer.allocate(Long.BYTES);
            buf.putLong(v.getTime());
            sk.update(buf);
            sk.updateSeparator();
        }
    }

    private static void writeDateFromTs(ApacheArrowWriter.Row row, String col, Timestamp v,
                                        boolean date32, SynthKey sk) {
        LocalDate ld = LocalDateTime.ofInstant(v.toInstant(), ZoneOffset.UTC).toLocalDate();
        if (date32) {
            row.writeDate32(col, ld);
        } else {
            row.writeDate(col, ld);
        }
        if (sk != null) {
            ByteBuffer buf = ByteBuffer.allocate(Long.BYTES);
            buf.putLong(v.getTime());
            sk.update(buf);
            sk.updateSeparator();
        }
    }

    private static void writeTimestampVal(ApacheArrowWriter.Row row, String col, Timestamp v,
                                          PrimitiveType type, SynthKey sk) {
        java.time.Instant inst = v.toInstant();
        LocalDateTime utcLocal = LocalDateTime.ofInstant(inst, ZoneOffset.UTC);
        if (type == PrimitiveType.Datetime) {
            row.writeDatetime(col, utcLocal);
        } else if (type == PrimitiveType.Timestamp) {
            row.writeTimestamp(col, inst);
        } else if (type == PrimitiveType.Datetime64) {
            row.writeDatetime64(col, utcLocal);
        } else {
            row.writeTimestamp64(col, inst);
        }
        if (sk != null) {
            ByteBuffer buf = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
            buf.putLong(v.getTime());
            buf.putInt(v.getNanos());
            sk.update(buf);
            sk.updateSeparator();
        }
    }


    private static boolean str2bool(String value) {
        if (value == null) {
            return false;
        }
        value = value.trim();
        if (value.length() == 0) {
            return false;
        }
        value = value.substring(0, 1);
        return !("N".equalsIgnoreCase(value) || "0".equalsIgnoreCase(value)
                || "F".equalsIgnoreCase(value)
                || "\u041D".equalsIgnoreCase(value) || "\u041B".equalsIgnoreCase(value));
    }

    private static int date2int(java.sql.Date date) {
        LocalDate ld = date.toLocalDate();
        return (ld.getYear() * 10000) + (ld.getMonthValue() * 100) + ld.getDayOfMonth();
    }

    private static int time2int(java.sql.Time time) {
        LocalTime lt = time.toLocalTime();
        return 3600 * lt.getHour() + 60 * lt.getMinute() + lt.getSecond();
    }

    private static String date2str(java.sql.Date date) {
        LocalDate ld = date.toLocalDate();
        return String.format("%d/%02d/%02d", ld.getYear(), ld.getMonthValue(), ld.getDayOfMonth());
    }


    private static List<ColumnMapping> buildColumnMappings(
            StructType paramType, ResultSetMetaData rsmd, TableDecision tab) throws Exception {
        List<ColumnMapping> result = new ArrayList<>();
        for (int rsIdx = 1; rsIdx <= rsmd.getColumnCount(); rsIdx++) {
            String columnName = rsmd.getColumnName(rsIdx);
            ColumnInfo ci = tab.getMetadata().findColumn(columnName);
            if (ci == null) {
                continue;
            }
            if (ColumnInfo.isBlob(ci.getSqlType())) {
                continue;
            }
            for (int si = 0; si < paramType.getMembersCount(); si++) {
                if (paramType.getMemberName(si).equals(ci.getDestinationName())) {
                    result.add(new ColumnMapping(rsIdx, si, ci.getSqlType()));
                    break;
                }
            }
        }
        return result;
    }

    private static List<BlobMapping> buildBlobMappings(
            StructType paramType, ResultSetMetaData rsmd, TableDecision tab,
            Map<String, BlobAccumulator> blobAccumulators) throws Exception {
        List<BlobMapping> result = new ArrayList<>();
        if (blobAccumulators == null || blobAccumulators.isEmpty()) {
            return result;
        }
        for (int rsIdx = 1; rsIdx <= rsmd.getColumnCount(); rsIdx++) {
            String columnName = rsmd.getColumnName(rsIdx);
            ColumnInfo ci = tab.getMetadata().findColumn(columnName);
            if (ci == null) {
                continue;
            }
            if (!ColumnInfo.isBlob(ci.getSqlType())) {
                continue;
            }
            BlobAccumulator acc = blobAccumulators.get(columnName);
            if (acc == null) {
                LOG.warn("Missing BlobAccumulator for BLOB column {} of source {}.{}",
                        columnName, tab.getSchema(), tab.getTable());
                continue;
            }
            for (int si = 0; si < paramType.getMembersCount(); si++) {
                if (paramType.getMemberName(si).equals(ci.getDestinationName())) {
                    result.add(new BlobMapping(rsIdx, si, acc));
                    break;
                }
            }
        }
        return result;
    }

    private static Type unwrapOptional(Type type) {
        while (Type.Kind.OPTIONAL.equals(type.getKind())) {
            type = type.unwrapOptional();
        }
        return type;
    }

    private static class ColumnMapping {
        final int rsIndex;
        final int structIndex;
        final int sqlType;

        ColumnMapping(int rsIndex, int structIndex, int sqlType) {
            this.rsIndex = rsIndex;
            this.structIndex = structIndex;
            this.sqlType = sqlType;
        }
    }

    private static class BlobMapping {
        final int rsIndex;
        final int structIndex;
        final BlobAccumulator accumulator;

        BlobMapping(int rsIndex, int structIndex, BlobAccumulator accumulator) {
            this.rsIndex = rsIndex;
            this.structIndex = structIndex;
            this.accumulator = accumulator;
        }
    }
}
