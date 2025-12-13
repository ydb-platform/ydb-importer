package tech.ydb.importer.integration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.result.ValueReader;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;
import tech.ydb.table.values.VoidValue;

/**
 * Client for reading schema and data from YDB.
 */
public final class TestYdbClient implements AutoCloseable {

    private final GrpcTransport transport;
    private final TableClient tableClient;
    private final SessionRetryContext retryCtx;
    private final String database;

    public TestYdbClient(String connectionString) {
        GrpcTransport tempTransport = null;
        try {
            tempTransport = GrpcTransport.forConnectionString(connectionString).build();
            this.database = tempTransport.getDatabase();
            this.tableClient = TableClient.newClient(tempTransport).build();
            this.retryCtx = SessionRetryContext.create(tableClient).build();
            this.transport = tempTransport;
        } catch (Exception e) {
            closeResource(tempTransport);
            throw new IllegalStateException("Failed to create TestYdbClient", e);
        }
    }

    public String getDatabase() {
        return database;
    }

    public TableDescription describeLogicalTable(String logicalName) {
        final String path = database + "/" + logicalName;
        return retryCtx
                .supplyResult(session -> session.describeTable(path))
                .join()
                .getValue();
    }

    /**
     * Read all rows from the specified logical table.
     * Note: we use expected table here to simplify loading all columns.
     * It will not produce false positives because we check database schema
     * independently with describe.
     */
    public List<RowData> readAll(ExpectedYdbTable expected) {
        final String path = database + "/" + expected.getFullName();

        Result<DataQueryResult> result = executeQuery(path);
        ResultSetReader resultSet = extractResultSet(result);

        Map<String, Integer> columnIndices = buildColumnIndexMap(resultSet);
        Map<String, ExpectedYdbColumn> expectedColumns = buildExpectedColumnMap(expected);
        Set<String> neededColumns = collectNeededColumns(expected);

        return readAllRows(resultSet, columnIndices, expectedColumns, neededColumns);
    }

    @Override
    public void close() {
        closeResource(tableClient);
        closeResource(transport);
    }

    /**
     * Execute query to read all rows from the specified table.
     */
    private Result<DataQueryResult> executeQuery(String tablePath) {
        String query = "SELECT * FROM `" + tablePath + "`;";
        return retryCtx.supplyResult(session -> session.executeDataQuery(query, TxControl.onlineRo())).join();
    }

    private ResultSetReader extractResultSet(Result<DataQueryResult> result) {
        Status status = result.getStatus();
        if (!status.isSuccess()) {
            throw new IllegalStateException("Data query failed: " + status);
        }

        DataQueryResult dataQueryResult = result.getValue();
        return dataQueryResult.getResultSet(0);
    }

    /**
     * Build a map from column name to its index in the result set.
     */
    private Map<String, Integer> buildColumnIndexMap(ResultSetReader resultSet) {
        Map<String, Integer> indices = new HashMap<>();
        int columnCount = resultSet.getColumnCount();

        for (int i = 0; i < columnCount; i++) {
            indices.put(resultSet.getColumnName(i), i);
        }

        return indices;
    }

    private Map<String, ExpectedYdbColumn> buildExpectedColumnMap(ExpectedYdbTable expected) {
        Map<String, ExpectedYdbColumn> map = new HashMap<>();
        for (ExpectedYdbColumn column : expected.getColumns()) {
            map.put(column.getName(), column);
        }
        return map;
    }

    /**
     * Collect column names that must be present in the result set.
     */
    private Set<String> collectNeededColumns(ExpectedYdbTable expected) {
        Set<String> needed = new HashSet<>(expected.getPrimaryKey());

        if (expected.getExpectedRows() != null) {
            for (ExpectedRow row : expected.getExpectedRows()) {
                needed.addAll(row.getValues().keySet());
            }
        }

        return needed;
    }

    /**
     * Read all rows from the result set.
     */
    private List<RowData> readAllRows(
            ResultSetReader resultSet,
            Map<String, Integer> columnIndices,
            Map<String, ExpectedYdbColumn> expectedColumns,
            Set<String> neededColumns) {

        List<RowData> rows = new ArrayList<>();

        while (resultSet.next()) {
            Map<String, Value<?>> rowValues = readRowValues(
                    resultSet, columnIndices, expectedColumns, neededColumns);
            rows.add(new RowData(rowValues));
        }

        return rows;
    }

    /**
     * Read values for all needed columns from the row.
     */
    private Map<String, Value<?>> readRowValues(
            ResultSetReader resultSet,
            Map<String, Integer> columnIndices,
            Map<String, ExpectedYdbColumn> expectedColumns,
            Set<String> neededColumns) {

        Map<String, Value<?>> values = new HashMap<>();

        for (String columnName : neededColumns) {
            ExpectedYdbColumn expectedColumn = expectedColumns.get(columnName);
            validateColumnExists(columnName, expectedColumn, expectedColumns.keySet());

            Integer columnIndex = columnIndices.get(columnName);
            validateColumnInResultSet(columnName, columnIndex, columnIndices.keySet());

            Value<?> columnValue = readColumnValue(resultSet, columnIndex, expectedColumn);
            values.put(columnName, columnValue);
        }

        return values;
    }

    /**
     * Read a value from the result set for the given column.
     */
    private Value<?> readColumnValue(
            ResultSetReader resultSet,
            int columnIndex,
            ExpectedYdbColumn expectedColumn) {

        ValueReader valueReader = resultSet.getColumn(columnIndex);
        Type columnType = resultSet.getColumnType(columnIndex);

        // Handle NULLs for optional columns.
        if (Type.Kind.OPTIONAL.equals(columnType.getKind())) {
            if (!valueReader.isOptionalItemPresent()) {
                return VoidValue.of();
            }
            valueReader = valueReader.getOptionalItem();
        }

        Type bareType = unwrapOptionalType(columnType);

        if (bareType.getKind() == Type.Kind.PRIMITIVE) {
            return readPrimitiveValue((PrimitiveType) bareType, valueReader);
        } else if (bareType.getKind() == Type.Kind.DECIMAL) {
            return readDecimalValue(valueReader);
        }

        throw new UnsupportedTypeException(
                "Unsupported type " + bareType + " for column " + expectedColumn.getName());
    }

    /**
     * Unwrap optional type until the base type is reached.
     */
    private Type unwrapOptionalType(Type type) {
        Type current = type;
        while (Type.Kind.OPTIONAL.equals(current.getKind())) {
            current = current.unwrapOptional();
        }
        return current;
    }

    /**
     * Read a primitive YDB value of a known primitive type.
     */
    private Value<?> readPrimitiveValue(PrimitiveType primitiveType, ValueReader valueReader) {
        switch (primitiveType) {
            case Int32:
                return PrimitiveValue.newInt32(valueReader.getInt32());
            case Int64:
                return PrimitiveValue.newInt64(valueReader.getInt64());
            case Text:
                return PrimitiveValue.newText(valueReader.getText());
            case Bool:
                return PrimitiveValue.newBool(valueReader.getBool());
            case Double:
                return PrimitiveValue.newDouble(valueReader.getDouble());
            case Float:
                return PrimitiveValue.newFloat(valueReader.getFloat());
            case Datetime64:
                return PrimitiveValue.newDatetime64(valueReader.getInt64());
            default:
                throw new UnsupportedTypeException(
                        "Unsupported primitive type " + primitiveType);
        }
    }

    private Value<?> readDecimalValue(ValueReader valueReader) {
        return valueReader.getDecimal();
    }

    /**
     * Ensure that the column is present in the expected columns map.
     */
    private void validateColumnExists(
            String columnName,
            ExpectedYdbColumn column,
            Set<String> expectedColumnNames) {

        if (column == null) {
            throw new IllegalStateException(
                    "Column '" + columnName + "' not found in expected columns. " +
                            "Expected columns: " + expectedColumnNames);
        }
    }

    /**
     * Ensure that the column is present in the result set.
     */
    private void validateColumnInResultSet(
            String columnName,
            Integer columnIndex,
            Set<String> resultSetColumns) {

        if (columnIndex == null) {
            throw new IllegalStateException(
                    "Column '" + columnName + "' not found in result set. " +
                            "Available columns: " + resultSetColumns);
        }
    }

    private void closeResource(AutoCloseable resource) {
        if (resource != null) {
            try {
                resource.close();
            } catch (Exception ignored) {

            }
        }
    }

    private static class UnsupportedTypeException extends IllegalStateException {
        UnsupportedTypeException(String message) {
            super(message);
        }
    }
}