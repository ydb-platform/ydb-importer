package tech.ydb.importer.integration.verification;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import tech.ydb.importer.integration.verification.TableScenario.PartitionStyle;

import static tech.ydb.importer.integration.verification.LogicalType.BOOL;
import static tech.ydb.importer.integration.verification.LogicalType.DATE;
import static tech.ydb.importer.integration.verification.LogicalType.DATETIME;
import static tech.ydb.importer.integration.verification.LogicalType.DECIMAL_18_4;
import static tech.ydb.importer.integration.verification.LogicalType.INT32;
import static tech.ydb.importer.integration.verification.LogicalType.INT64;
import static tech.ydb.importer.integration.verification.LogicalType.NULLABLE_STRING;
import static tech.ydb.importer.integration.verification.LogicalType.STRING;

/** Shop scenarios for cross-DB verification */
public final class ShopScenarios {

    private static final LocalDate BASE_DATE = LocalDate.of(2020, 1, 1);
    private static final LocalDateTime BASE_DT =
            LocalDateTime.of(2024, 1, 1, 0, 0, 0);
    private static final String[] CURRENCIES = {"USD", "EUR", "RUB"};
    private static final String[] METHODS = {"card", "cash", "transfer", "crypto"};
    private static final String[] EVENT_TYPES =
            {"view", "click", "purchase", "logout", "error"};
    private static final String[] ENTITY_TYPES =
            {"user", "order", "product", "payment", "shipment"};
    private static final long BLOB_ROW_COUNT = 1000;

    private ShopScenarios() {
    }

    public static TableScenario users(long n) {
        return Scenario.table("users", n)
                .col("id",              INT64,           id -> id)
                .col("email",           STRING,          id -> "user" + id + "@example.com")
                .col("name",            STRING,          id -> "Клиент №" + id)
                .col("is_verified",     BOOL,            id -> id % 3 != 0)
                .col("registered_date", DATE,            id -> BASE_DATE.plusDays(id % 1460))
                .col("bio",             NULLABLE_STRING, id -> id % 5 == 0 ? null : "Bio of user " + id)
                .build();
    }

    public static TableScenario categories(long n) {
        return Scenario.table("categories", n)
                .col("id",        INT64,  id -> id)
                .col("name",      STRING, id -> "Category-" + id)
                .col("parent_id", INT64,  id -> id <= 10 ? 0L : ((id - 1) % 10) + 1)
                .build();
    }

    public static TableScenario products(long n) {
        return Scenario.table("products", n)
                .col("id",           INT64,        id -> id)
                .col("category_id",  INT64,        id -> (id % 100) + 1)
                .col("title",        STRING,       id -> "Product #" + id)
                .col("price",        DECIMAL_18_4, id -> BigDecimal.valueOf((id % 10000) * 100 + 99, 2)
                        .setScale(4, RoundingMode.UNNECESSARY))
                .col("weight_grams", INT32,        id -> (int) ((id * 37) % 50000))
                .col("is_available", BOOL,         id -> id % 7 != 0)
                .build();
    }

    public static TableScenario inventory(long n) {
        return Scenario.table("inventory", n)
                .col("id",           INT64,    id -> id)
                .col("product_id",   INT64,    id -> (id % n) + 1)
                .col("warehouse_id", INT32,    id -> (int) (id % 50))
                .col("quantity",     INT32,    id -> (int) ((id * 13) % 10000))
                .col("updated_at",   DATETIME, id -> BASE_DT.plusSeconds(id))
                .partition(PartitionStyle.HASH_INT, "warehouse_id")
                .build();
    }

    public static TableScenario orders(long n, long userCount) {
        final String[] statuses = {"new", "paid", "shipped", "done"};
        return Scenario.table("orders", n)
                .col("id",        INT64,        id -> id)
                .col("user_id",   INT64,        id -> (id % userCount) + 1)
                .col("total",     DECIMAL_18_4, id -> BigDecimal.valueOf(id * 13, 2)
                        .setScale(4, RoundingMode.UNNECESSARY))
                .col("currency",  STRING,       id -> CURRENCIES[(int) (id % 3)])
                .col("placed_at", DATETIME,     id -> BASE_DT.plusSeconds(id))
                .col("status",    STRING,       id -> statuses[(int) (id % 4)])
                .partition(PartitionStyle.RANGE_DATE, "placed_at")
                .build();
    }

    public static TableScenario orderItems(long n, long orderCount) {
        return Scenario.table("order_items", n)
                .col("id",         INT64,        id -> id)
                .col("order_id",   INT64,        id -> (id % orderCount) + 1)
                .col("product_id", INT64,        id -> (id * 7 % n) + 1)
                .col("qty",        INT32,        id -> (int) ((id % 10) + 1))
                .col("unit_price", DECIMAL_18_4, id -> BigDecimal.valueOf((id % 5000) * 100 + 50, 2)
                        .setScale(4, RoundingMode.UNNECESSARY))
                .partition(PartitionStyle.RANGE_INT, "order_id")
                .build();
    }

    public static TableScenario payments(long n, long orderCount) {
        return Scenario.table("payments", n)
                .col("id",       INT64,        id -> id)
                .col("order_id", INT64,        id -> (id % orderCount) + 1)
                .col("amount",   DECIMAL_18_4, id -> BigDecimal.valueOf(id * 11, 2)
                        .setScale(4, RoundingMode.UNNECESSARY))
                .col("method",   STRING,       id -> METHODS[(int) (id % 4)])
                .col("paid_at",  DATETIME,     id -> BASE_DT.plusSeconds(id + 30))
                .partition(PartitionStyle.LIST_STRING, "method")
                .build();
    }

    public static TableScenario reviews(long n, long userCount) {
        return Scenario.table("reviews", n)
                .col("id",          INT64,    id -> id)
                .col("product_id",  INT64,    id -> (id % n) + 1)
                .col("user_id",     INT64,    id -> (id % userCount) + 1)
                .col("rating",      INT32,    id -> (int) (id % 5) + 1)
                .col("review_text", STRING,   id -> "Отзыв №" + id)
                .col("reviewed_at", DATETIME, id -> BASE_DT.plusSeconds(id * 2))
                .build();
    }

    public static TableScenario addresses(long n, long userCount) {
        final String[] cities =
                {"Москва", "Санкт-Петербург", "Казань", "Новосибирск", "Екатеринбург"};
        return Scenario.table("addresses", n)
                .col("id",          INT64,  id -> id)
                .col("user_id",     INT64,  id -> (id % userCount) + 1)
                .col("city",        STRING, id -> cities[(int) (id % 5)])
                .col("street",      STRING, id -> "ул. Тестовая, д. " + id)
                .col("postal_code", STRING, id -> String.format("%06d", id % 200000))
                .col("is_default",  BOOL,   id -> id % 3 == 0)
                .build();
    }

    public static TableScenario shipments(long n, long orderCount, long addressCount) {
        return Scenario.table("shipments", n)
                .col("id",            INT64,           id -> id)
                .col("order_id",      INT64,           id -> (id % orderCount) + 1)
                .col("address_id",    INT64,           id -> (id % addressCount) + 1)
                .col("shipped_at",    DATETIME,        id -> BASE_DT.plusSeconds(id + 60))
                .col("tracking_code", NULLABLE_STRING, id -> id % 4 == 0 ? null : "TRK-" + id)
                .partition(PartitionStyle.HASH_INT, "address_id")
                .build();
    }

    public static TableScenario productImages(long n) {
        return Scenario.table("product_images", n)
                .col("id",         INT64,  id -> id)
                .col("product_id", INT64,  id -> (id % n) + 1)
                .col("caption",    STRING, id -> "Image #" + id)
                .blob("image_data", ShopScenarios::imageBytes)
                .build();
    }

    private static byte[] imageBytes(long id) {
        if (id % 10 == 0) {
            return null;
        }
        int size;
        if (id % 3 == 0) {
            size = 200;
        } else if (id % 3 == 1) {
            size = 80_000;
        } else {
            size = 200_000;
        }
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = (byte) ((id * 31 + i) % 256);
        }
        return data;
    }

    public static TableScenario auditLog(long n) {
        return Scenario.table("audit_log", n)
                .col("id",          INT64,    id -> id)
                .col("entity_type", STRING,   id -> ENTITY_TYPES[(int) (id % 5)])
                .col("entity_id",   INT64,    id -> id * 3)
                .col("action_type", STRING,   id -> EVENT_TYPES[(int) (id % 5)])
                .col("ts",          DATETIME, id -> BASE_DT.plusSeconds(id))
                .partition(PartitionStyle.RANGE_DATE, "ts")
                .build();
    }

    public static List<TableScenario> all(long n) {
        List<TableScenario> list = new ArrayList<>();
        list.add(users(n));
        list.add(categories(n));
        list.add(products(n));
        list.add(inventory(n));
        list.add(orders(n, n));
        list.add(orderItems(n, n));
        list.add(payments(n, n));
        list.add(reviews(n, n));
        list.add(addresses(n, n));
        list.add(shipments(n, n, n));
        list.add(productImages(BLOB_ROW_COUNT));
        list.add(auditLog(n));
        return list;
    }
}
