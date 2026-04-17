package tech.ydb.importer.integration.verification;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static tech.ydb.importer.integration.verification.LogicalType.*;

public final class ShopScenarios {

    private static final LocalDate BASE_DATE = LocalDate.of(2020, 1, 1);
    private static final LocalDateTime BASE_DT =
            LocalDateTime.of(2024, 1, 1, 0, 0, 0);
    private static final String[] CURRENCIES = {"USD", "EUR", "RUB"};
    private static final String[] METHODS = {"card", "cash", "transfer", "crypto"};
    private static final String[] EVENT_TYPES =
            {"view", "click", "purchase", "logout", "error"};
    private static final long BLOB_ROW_COUNT = 1000;
    private static final String[] ENTITY_TYPES =
            {"user", "order", "product", "payment", "shipment"};

    private ShopScenarios() {
    }

    // 1. users - STRING, BOOL, DATE, NULLABLE_STRING
    public static TableScenario users(long n) {
        List<ColumnSpec> cols = Arrays.asList(
                new ColumnSpec("id", INT64),
                new ColumnSpec("email", STRING),
                new ColumnSpec("name", STRING),
                new ColumnSpec("is_verified", BOOL),
                new ColumnSpec("registered_date", DATE),
                new ColumnSpec("bio", NULLABLE_STRING)
        );
        return new TableScenario("users", cols, new RowOracle() {
            @Override public long rowCount() { return n; }
            @Override public Map<String, Object> expectedFor(long id) {
                Map<String, Object> r = new HashMap<>();
                r.put("id", id);
                r.put("email", "user" + id + "@example.com");
                r.put("name", "Клиент №" + id);
                r.put("is_verified", id % 3 != 0);
                r.put("registered_date", BASE_DATE.plusDays(id % 1460));
                r.put("bio", id % 5 == 0 ? null : "Bio of user " + id);
                return r;
            }
        });
    }

    // 2. categories - INT64 (parent_id = hierarchy)
    public static TableScenario categories(long n) {
        List<ColumnSpec> cols = Arrays.asList(
                new ColumnSpec("id", INT64),
                new ColumnSpec("name", STRING),
                new ColumnSpec("parent_id", INT64)
        );
        return new TableScenario("categories", cols, new RowOracle() {
            @Override public long rowCount() { return n; }
            @Override public Map<String, Object> expectedFor(long id) {
                Map<String, Object> r = new HashMap<>();
                r.put("id", id);
                r.put("name", "Category-" + id);
                r.put("parent_id", id <= 10 ? 0L : ((id - 1) % 10) + 1);
                return r;
            }
        });
    }

    // 3. products - DECIMAL, INT32, BOOL
    public static TableScenario products(long n) {
        List<ColumnSpec> cols = Arrays.asList(
                new ColumnSpec("id", INT64),
                new ColumnSpec("category_id", INT64),
                new ColumnSpec("title", STRING),
                new ColumnSpec("price", DECIMAL_18_4),
                new ColumnSpec("weight_grams", INT32),
                new ColumnSpec("is_available", BOOL)
        );
        return new TableScenario("products", cols, new RowOracle() {
            @Override public long rowCount() { return n; }
            @Override public Map<String, Object> expectedFor(long id) {
                Map<String, Object> r = new HashMap<>();
                r.put("id", id);
                r.put("category_id", (id % 100) + 1);
                r.put("title", "Product #" + id);
                r.put("price", BigDecimal.valueOf(
                        (id % 10000) * 100 + 99, 2)
                        .setScale(4, RoundingMode.UNNECESSARY));
                r.put("weight_grams", (int) ((id * 37) % 50000));
                r.put("is_available", id % 7 != 0);
                return r;
            }
        });
    }

    // 4. inventory - PARTITIONED HASH_INT by warehouse_id
    public static TableScenario inventory(long n) {
        List<ColumnSpec> cols = Arrays.asList(
                new ColumnSpec("id", INT64),
                new ColumnSpec("product_id", INT64),
                new ColumnSpec("warehouse_id", INT32),
                new ColumnSpec("quantity", INT32),
                new ColumnSpec("updated_at", DATETIME)
        );
        return new TableScenario("inventory", cols, new RowOracle() {
            @Override public long rowCount() { return n; }
            @Override public Map<String, Object> expectedFor(long id) {
                Map<String, Object> r = new HashMap<>();
                r.put("id", id);
                r.put("product_id", (id % n) + 1);
                r.put("warehouse_id", (int) (id % 50));
                r.put("quantity", (int) ((id * 13) % 10000));
                r.put("updated_at", BASE_DT.plusSeconds(id));
                return r;
            }
        }, null, PartitionStyle.HASH_INT, "warehouse_id");
    }

    // 5. orders - PARTITIONED RANGE_DATE by placed_at
    public static TableScenario orders(long n, long userCount) {
        List<ColumnSpec> cols = Arrays.asList(
                new ColumnSpec("id", INT64),
                new ColumnSpec("user_id", INT64),
                new ColumnSpec("total", DECIMAL_18_4),
                new ColumnSpec("currency", STRING),
                new ColumnSpec("placed_at", DATETIME),
                new ColumnSpec("status", STRING)
        );
        final String[] statuses = {"new", "paid", "shipped", "done"};
        return new TableScenario("orders", cols, new RowOracle() {
            @Override public long rowCount() { return n; }
            @Override public Map<String, Object> expectedFor(long id) {
                Map<String, Object> r = new HashMap<>();
                r.put("id", id);
                r.put("user_id", (id % userCount) + 1);
                r.put("total", BigDecimal.valueOf(id * 13, 2)
                        .setScale(4, RoundingMode.UNNECESSARY));
                r.put("currency", CURRENCIES[(int) (id % 3)]);
                r.put("placed_at", BASE_DT.plusSeconds(id));
                r.put("status", statuses[(int) (id % 4)]);
                return r;
            }
        }, null, PartitionStyle.RANGE_DATE, "placed_at");
    }

    // 6. order_items - PARTITIONED RANGE_INT by order_id
    public static TableScenario orderItems(long n, long orderCount) {
        List<ColumnSpec> cols = Arrays.asList(
                new ColumnSpec("id", INT64),
                new ColumnSpec("order_id", INT64),
                new ColumnSpec("product_id", INT64),
                new ColumnSpec("qty", INT32),
                new ColumnSpec("unit_price", DECIMAL_18_4)
        );
        return new TableScenario("order_items", cols, new RowOracle() {
            @Override public long rowCount() { return n; }
            @Override public Map<String, Object> expectedFor(long id) {
                Map<String, Object> r = new HashMap<>();
                r.put("id", id);
                r.put("order_id", (id % orderCount) + 1);
                r.put("product_id", (id * 7 % n) + 1);
                r.put("qty", (int) ((id % 10) + 1));
                r.put("unit_price", BigDecimal.valueOf(
                        (id % 5000) * 100 + 50, 2)
                        .setScale(4, RoundingMode.UNNECESSARY));
                return r;
            }
        }, null, PartitionStyle.RANGE_INT, "order_id");
    }

    // 7. payments - PARTITIONED LIST_STRING by method
    public static TableScenario payments(long n, long orderCount) {
        List<ColumnSpec> cols = Arrays.asList(
                new ColumnSpec("id", INT64),
                new ColumnSpec("order_id", INT64),
                new ColumnSpec("amount", DECIMAL_18_4),
                new ColumnSpec("method", STRING),
                new ColumnSpec("paid_at", DATETIME)
        );
        return new TableScenario("payments", cols, new RowOracle() {
            @Override public long rowCount() { return n; }
            @Override public Map<String, Object> expectedFor(long id) {
                Map<String, Object> r = new HashMap<>();
                r.put("id", id);
                r.put("order_id", (id % orderCount) + 1);
                r.put("amount", BigDecimal.valueOf(id * 11, 2)
                        .setScale(4, RoundingMode.UNNECESSARY));
                r.put("method", METHODS[(int) (id % 4)]);
                r.put("paid_at", BASE_DT.plusSeconds(id + 30));
                return r;
            }
        }, null, PartitionStyle.LIST_STRING, "method");
    }

    // 8. reviews - INT32 (rating 1-5), STRING, DATETIME
    public static TableScenario reviews(long n, long userCount) {
        List<ColumnSpec> cols = Arrays.asList(
                new ColumnSpec("id", INT64),
                new ColumnSpec("product_id", INT64),
                new ColumnSpec("user_id", INT64),
                new ColumnSpec("rating", INT32),
                new ColumnSpec("review_text", STRING),
                new ColumnSpec("reviewed_at", DATETIME)
        );
        return new TableScenario("reviews", cols, new RowOracle() {
            @Override public long rowCount() { return n; }
            @Override public Map<String, Object> expectedFor(long id) {
                Map<String, Object> r = new HashMap<>();
                r.put("id", id);
                r.put("product_id", (id % n) + 1);
                r.put("user_id", (id % userCount) + 1);
                r.put("rating", (int) (id % 5) + 1);
                r.put("review_text", "Отзыв №" + id);
                r.put("reviewed_at", BASE_DT.plusSeconds(id * 2));
                return r;
            }
        });
    }

    // 9. addresses - STRING x3, BOOL
    public static TableScenario addresses(long n, long userCount) {
        List<ColumnSpec> cols = Arrays.asList(
                new ColumnSpec("id", INT64),
                new ColumnSpec("user_id", INT64),
                new ColumnSpec("city", STRING),
                new ColumnSpec("street", STRING),
                new ColumnSpec("postal_code", STRING),
                new ColumnSpec("is_default", BOOL)
        );
        final String[] cities =
                {"Москва", "Санкт-Петербург", "Казань", "Новосибирск", "Екатеринбург"};
        return new TableScenario("addresses", cols, new RowOracle() {
            @Override public long rowCount() { return n; }
            @Override public Map<String, Object> expectedFor(long id) {
                Map<String, Object> r = new HashMap<>();
                r.put("id", id);
                r.put("user_id", (id % userCount) + 1);
                r.put("city", cities[(int) (id % 5)]);
                r.put("street", "ул. Тестовая, д. " + id);
                r.put("postal_code", String.format("%06d", id % 200000));
                r.put("is_default", id % 3 == 0);
                return r;
            }
        });
    }

    // 10. shipments - PARTITIONED HASH_INT by address_id, NULLABLE_STRING
    public static TableScenario shipments(long n, long orderCount,
                                          long addressCount) {
        List<ColumnSpec> cols = Arrays.asList(
                new ColumnSpec("id", INT64),
                new ColumnSpec("order_id", INT64),
                new ColumnSpec("address_id", INT64),
                new ColumnSpec("shipped_at", DATETIME),
                new ColumnSpec("tracking_code", NULLABLE_STRING)
        );
        return new TableScenario("shipments", cols, new RowOracle() {
            @Override public long rowCount() { return n; }
            @Override public Map<String, Object> expectedFor(long id) {
                Map<String, Object> r = new HashMap<>();
                r.put("id", id);
                r.put("order_id", (id % orderCount) + 1);
                r.put("address_id", (id % addressCount) + 1);
                r.put("shipped_at", BASE_DT.plusSeconds(id + 60));
                r.put("tracking_code",
                        id % 4 == 0 ? null : "TRK-" + id);
                return r;
            }
        }, null, PartitionStyle.HASH_INT, "address_id");
    }

    // 11. product_images - BLOB
    public static TableScenario productImages(long n) {
        List<ColumnSpec> cols = Arrays.asList(
                new ColumnSpec("id", INT64),
                new ColumnSpec("product_id", INT64),
                new ColumnSpec("caption", STRING)
        );
        return new TableScenario("product_images", cols, new RowOracle() {
            @Override public long rowCount() { return n; }
            @Override public Map<String, Object> expectedFor(long id) {
                Map<String, Object> r = new HashMap<>();
                r.put("id", id);
                r.put("product_id", (id % n) + 1);
                r.put("caption", "Image #" + id);
                return r;
            }
            @Override public byte[] expectedBlobFor(long id) {
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
        }, "image_data", null, null);
    }

    // 12. audit_log - PARTITIONED RANGE_DATE by ts
    public static TableScenario auditLog(long n) {
        List<ColumnSpec> cols = Arrays.asList(
                new ColumnSpec("id", INT64),
                new ColumnSpec("entity_type", STRING),
                new ColumnSpec("entity_id", INT64),
                new ColumnSpec("action_type", STRING),
                new ColumnSpec("ts", DATETIME)
        );
        return new TableScenario("audit_log", cols, new RowOracle() {
            @Override public long rowCount() { return n; }
            @Override public Map<String, Object> expectedFor(long id) {
                Map<String, Object> r = new HashMap<>();
                r.put("id", id);
                r.put("entity_type",
                        ENTITY_TYPES[(int) (id % 5)]);
                r.put("entity_id", id * 3);
                r.put("action_type", EVENT_TYPES[(int) (id % 5)]);
                r.put("ts", BASE_DT.plusSeconds(id));
                return r;
            }
        }, null, PartitionStyle.RANGE_DATE, "ts");
    }

    // all 12 scenarios
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
