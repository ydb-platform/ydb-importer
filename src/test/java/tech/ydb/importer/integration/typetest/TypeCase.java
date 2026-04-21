package tech.ydb.importer.integration.typetest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import tech.ydb.table.values.Type;

/** One column entry in a type-mapping test */
public final class TypeCase {

    private final String columnName;
    private final String sourceType;
    private final Type expectedYdbType;
    private final List<ValueCase> values;

    TypeCase(String columnName, String sourceType, Type expectedYdbType,
             List<ValueCase> values) {
        this.columnName = columnName;
        this.sourceType = sourceType;
        this.expectedYdbType = expectedYdbType;
        this.values = Collections.unmodifiableList(new ArrayList<>(values));
    }

    public String getColumnName() {
        return columnName;
    }

    public String getSourceType() {
        return sourceType;
    }

    public Type getExpectedYdbType() {
        return expectedYdbType;
    }

    public List<ValueCase> getValues() {
        return values;
    }

    public static final class ValueCase {

        private final String insertLiteral;
        private final Object expectedValue;

        ValueCase(String insertLiteral, Object expectedValue) {
            this.insertLiteral = insertLiteral;
            this.expectedValue = expectedValue;
        }

        public String getInsertLiteral() {
            return insertLiteral;
        }

        public Object getExpectedValue() {
            return expectedValue;
        }
    }
}
