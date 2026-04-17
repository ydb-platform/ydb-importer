package tech.ydb.importer.integration.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import tech.ydb.core.Result;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import tech.ydb.table.description.TableColumn;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.result.ValueReader;
import tech.ydb.table.settings.ExecuteScanQuerySettings;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.Type;

/**
 * Lightweight YDB access helper for test assertions and cleanup.
 */
public final class YdbSchemaReader implements AutoCloseable {

    private final GrpcTransport transport;
    private final TableClient tableClient;
    private final SessionRetryContext retryCtx;
    private final String database;

    public YdbSchemaReader(String connectionString) {
        GrpcTransport tempTransport = null;
        try {
            tempTransport = GrpcTransport.forConnectionString(connectionString).build();
            this.database = tempTransport.getDatabase();
            TableClient client = TableClient.newClient(tempTransport).build();
            this.tableClient = client;
            this.retryCtx = SessionRetryContext.create(client).build();
            this.transport = tempTransport;
        } catch (RuntimeException e) {
            closeQuietly(tempTransport);
            throw e;
        }
    }

    public String getDatabase() {
        return database;
    }

    /**
     * Describes a logical table by its relative path (without the database prefix).
     */
    public YdbTableInfo describe(String logicalPath) {
        String fullPath = database + "/" + logicalPath;
        TableDescription desc = retryCtx
                .supplyResult(session -> session.describeTable(fullPath))
                .join()
                .getValue();

        List<YdbColumn> columns = new ArrayList<>();
        for (TableColumn col : desc.getColumns()) {
            columns.add(new YdbColumn(col.getName(), col.getType()));
        }
        return new YdbTableInfo(columns, desc.getPrimaryKeys());
    }

    /**
     * Executes a YQL data query and returns the first result set.
     */
    public ResultSetReader executeDataQuery(String yql) {
        Result<DataQueryResult> result = retryCtx
                .supplyResult(session -> session.executeDataQuery(yql, TxControl.onlineRo()))
                .join();
        if (!result.getStatus().isSuccess()) {
            throw new IllegalStateException("YDB query failed: " + result.getStatus());
        }
        return result.getValue().getResultSet(0);
    }

    public void executeDml(String yql) {
        retryCtx.supplyResult(session ->
                session.executeDataQuery(yql,
                        TxControl.serializableRw().setCommitTx(true)))
                .join().getStatus().expectSuccess(
                        "DML failed: " + yql);
    }

    public long countRows(String logicalPath) {
        String fullPath = database + "/" + logicalPath;
        String yql = "SELECT COUNT(*) AS cnt FROM `" + fullPath + "`;";
        ResultSetReader rs = executeDataQuery(yql);
        if (!rs.next()) {
            throw new IllegalStateException("COUNT query returned no rows");
        }
        return rs.getColumn(0).getUint64();
    }

    /**
     * Best-effort table drop by logical path. Swallows exceptions so it's
     * safe in {@code finally} blocks.
     */
    public void dropTable(String logicalPath) {
        try {
            String fullPath = database + "/" + logicalPath;
            retryCtx.supplyStatus(session -> session.dropTable(fullPath)).join();
        } catch (Exception ignored) {
        }
    }

    /**
     * Reassembles BLOB bytes from the aux table for the row identified by
     * {@code pkColumn = pkValue}. Returns {@code null} if the BLOB is NULL.
     */
    public byte[] readBlobBytes(String mainPath, String blobColumn,
                                String pkColumn, Object pkValue) {
        String mainFull = database + "/" + mainPath;
        String idYql = "SELECT " + blobColumn + " FROM `" + mainFull
                + "` WHERE " + pkColumn + " = " + ydbLiteral(pkValue) + ";";
        ResultSetReader rs = executeDataQuery(idYql);
        if (!rs.next()) {
            return null;
        }
        Object idValue = readValue(rs.getColumn(0), rs.getColumnType(0));
        if (idValue == null) {
            return null;
        }
        long id = ((Number) idValue).longValue();
        String auxFull = database + "/" + mainPath + "_" + blobColumn;
        ResultSetReader chunks = executeDataQuery(
                "SELECT val FROM `" + auxFull + "` WHERE id = " + id
                + " ORDER BY pos;");
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        while (chunks.next()) {
            Object v = readValue(chunks.getColumn(0), chunks.getColumnType(0));
            if (v != null) {
                byte[] bytes = (byte[]) v;
                out.write(bytes, 0, bytes.length);
            }
        }
        return out.toByteArray();
    }

    public String readClobText(String mainPath, String clobColumn,
                               String pkColumn, Object pkValue) {
        String mainFull = database + "/" + mainPath;
        String idYql = "SELECT " + clobColumn + " FROM `" + mainFull
                + "` WHERE " + pkColumn + " = " + ydbLiteral(pkValue) + ";";
        ResultSetReader rs = executeDataQuery(idYql);
        if (!rs.next()) {
            return null;
        }
        Object idValue = readValue(rs.getColumn(0), rs.getColumnType(0));
        if (idValue == null) {
            return null;
        }
        long id = ((Number) idValue).longValue();
        String auxFull = database + "/" + mainPath + "_" + clobColumn;
        ResultSetReader chunks = executeDataQuery(
                "SELECT val FROM `" + auxFull + "` WHERE id = " + id
                + " ORDER BY pos;");
        StringBuilder out = new StringBuilder();
        while (chunks.next()) {
            Object v = readValue(chunks.getColumn(0), chunks.getColumnType(0));
            if (v != null) {
                out.append((String) v);
            }
        }
        return out.toString();
    }

    private static String ydbLiteral(Object v) {
        if (v instanceof Number) {
            return v.toString();
        }
        return "\"" + v.toString().replace("\"", "\\\"") + "\"";
    }

    public List<Map<String, Object>> readRows(String logicalPath,
                                              String orderByColumn) {
        String fullPath = database + "/" + logicalPath;
        String yql = "SELECT * FROM `" + fullPath + "` ORDER BY "
                + orderByColumn + ";";
        ResultSetReader rs = executeDataQuery(yql);
        int columnCount = rs.getColumnCount();
        List<Map<String, Object>> rows = new ArrayList<>();
        while (rs.next()) {
            Map<String, Object> row = new HashMap<>();
            for (int i = 0; i < columnCount; i++) {
                row.put(rs.getColumnName(i),
                        readValue(rs.getColumn(i), rs.getColumnType(i)));
            }
            rows.add(row);
        }
        return rows;
    }

    public void streamRows(String logicalPath,
                           Consumer<Map<String, Object>> rowHandler) {
        String fullPath = database + "/" + logicalPath;
        String yql = "SELECT * FROM `" + fullPath + "`;";
        retryCtx.supplyStatus(session ->
                session.executeScanQuery(yql, Params.empty(),
                        ExecuteScanQuerySettings.newBuilder().build())
                        .start(part -> {
                            int cols = part.getColumnCount();
                            while (part.next()) {
                                Map<String, Object> row = new HashMap<>();
                                for (int i = 0; i < cols; i++) {
                                    row.put(part.getColumnName(i)
                                                    .toLowerCase(),
                                            readValue(part.getColumn(i),
                                                    part.getColumnType(i)));
                                }
                                rowHandler.accept(row);
                            }
                        })
        ).join().expectSuccess("Scan query failed for " + logicalPath);

    }

    private static Object readValue(ValueReader reader, Type type) {
        Type current = type;
        ValueReader currentReader = reader;
        if (current.getKind() == Type.Kind.OPTIONAL) {
            if (!currentReader.isOptionalItemPresent()) {
                return null;
            }
            currentReader = currentReader.getOptionalItem();
            current = current.unwrapOptional();
        }
        if (current.getKind() == Type.Kind.PRIMITIVE) {
            return readPrimitive((PrimitiveType) current, currentReader);
        }
        if (current.getKind() == Type.Kind.DECIMAL) {
            return currentReader.getDecimal().toBigDecimal();
        }
        throw new IllegalStateException(
                "Unsupported YDB type kind: " + current.getKind());
    }

    private static Object readPrimitive(PrimitiveType type,
                                        ValueReader reader) {
        switch (type) {
            case Bool:        return reader.getBool();
            case Int8:        return (int) reader.getInt8();
            case Uint8:       return reader.getUint8();
            case Int16:       return (int) reader.getInt16();
            case Uint16:      return reader.getUint16();
            case Int32:       return reader.getInt32();
            case Uint32:      return reader.getUint32();
            case Int64:       return reader.getInt64();
            case Uint64:      return reader.getUint64();
            case Float:       return reader.getFloat();
            case Double:      return reader.getDouble();
            case Bytes:       return reader.getBytes();
            case Text:        return reader.getText();
            case Date:        return reader.getDate();
            case Date32:      return reader.getDate32();
            case Datetime:    return reader.getDatetime();
            case Datetime64:  return reader.getDatetime64();
            case Timestamp:   return reader.getTimestamp();
            case Timestamp64: return reader.getTimestamp64();
            case Uuid:        return reader.getUuid();
            default:
                throw new IllegalStateException(
                        "Unsupported primitive type: " + type);
        }
    }

    @Override
    public void close() {
        closeQuietly(tableClient);
        closeQuietly(transport);
    }

    private static void closeQuietly(AutoCloseable resource) {
        if (resource != null) {
            try {
                resource.close();
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * Plain structural description of a YDB table.
     */
    public static final class YdbTableInfo {

        private final Map<String, YdbColumn> byName;
        private final List<String> primaryKey;

        YdbTableInfo(List<YdbColumn> columns, List<String> primaryKey) {
            this.primaryKey = Collections.unmodifiableList(new ArrayList<>(primaryKey));
            Map<String, YdbColumn> index = new HashMap<>();
            for (YdbColumn c : columns) {
                index.put(c.getName(), c);
            }
            this.byName = Collections.unmodifiableMap(index);
        }

        public List<String> getPrimaryKey() {
            return primaryKey;
        }

        public YdbColumn getColumn(String name) {
            YdbColumn col = byName.get(name);
            if (col == null) {
                throw new IllegalArgumentException(
                        "Column '" + name + "' not found"
                        + "; available columns: " + byName.keySet());
            }
            return col;
        }

        public boolean hasColumn(String name) {
            return byName.containsKey(name);
        }
    }

    /**
     * One YDB column with its declared type.
     */
    public static final class YdbColumn {

        private final String name;
        private final Type rawType;

        YdbColumn(String name, Type rawType) {
            this.name = name;
            this.rawType = rawType;
        }

        public String getName() {
            return name;
        }

        public Type getRawType() {
            return rawType;
        }

        /**
         * Returns the bare (non-optional) type for use in assertions.
         */
        public Type getBareType() {
            return Type.Kind.OPTIONAL.equals(rawType.getKind())
                    ? rawType.unwrapOptional()
                    : rawType;
        }

    }
}
