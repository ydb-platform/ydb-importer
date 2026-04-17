package tech.ydb.importer.integration.verification;

public enum PartitionStyle {
    HASH_INT,
    RANGE_INT,
    RANGE_DATE,
    LIST_STRING
}
