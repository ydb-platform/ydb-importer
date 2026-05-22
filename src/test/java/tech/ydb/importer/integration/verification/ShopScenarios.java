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
import static tech.ydb.importer.integration.verification.LogicalType.STRING;
import static tech.ydb.importer.integration.verification.ScenarioRandom.pickFromArray;
import static tech.ydb.importer.integration.verification.ScenarioRandom.salt;
import static tech.ydb.importer.integration.verification.ScenarioRandom.stableHash;
import static tech.ydb.importer.integration.verification.ScenarioRandom.weighted;
import static tech.ydb.importer.integration.verification.ScenarioRandom.zipfLikePick;

/** Shop scenarios for cross-DB verification */
public final class ShopScenarios {

    private static final LocalDate BASE_DATE = LocalDate.of(2020, 1, 1);
    static final LocalDateTime BASE_DT =
            LocalDateTime.of(2024, 1, 1, 0, 0, 0);

    /** Time range for generated timestamps (2 years). */
    static final long TARGET_SECONDS = 2L * 365L * 86400L;

    private static final double ZIPF_ALPHA = 2.0;

    private static final long BLOB_ROW_COUNT = 1000;

    // Separate salt per column for independent values.
    private static final long FK_USER       = salt("fk_user");
    private static final long FK_PRODUCT    = salt("fk_product");
    private static final long FK_ORDER      = salt("fk_order");
    private static final long FK_ADDRESS    = salt("fk_address");
    private static final long FK_CATEGORY   = salt("fk_category");
    private static final long FK_WAREHOUSE  = salt("fk_warehouse");

    private static final long T_PLACED      = salt("placed_at");
    private static final long T_PAID        = salt("paid_at");
    private static final long T_SHIPPED     = salt("shipped_at");
    private static final long T_REVIEWED    = salt("reviewed_at");
    private static final long T_UPDATE      = salt("updated_at");
    private static final long T_TS          = salt("ts");
    private static final long T_DELETED     = salt("deleted_at");
    private static final long T_REGISTERED  = salt("registered_date");

    private static final long CAT_METHOD    = salt("method");
    private static final long CAT_CURRENCY  = salt("currency");
    private static final long CAT_STATUS    = salt("status");
    private static final long CAT_EVENT     = salt("action_type");
    private static final long CAT_ENTITY    = salt("entity_type");

    private static final long TXT_CITY        = salt("city");
    private static final long TXT_STREET_PFX  = salt("street_prefix");
    private static final long TXT_STREET_ROOT = salt("street_root");
    private static final long TXT_FIRST       = salt("first_name");
    private static final long TXT_LAST        = salt("last_name");
    private static final long TXT_HOUSE       = salt("house");

    private static final long B_VERIFIED    = salt("is_verified");
    private static final long B_AVAILABLE   = salt("is_available");
    private static final long B_DEFAULT     = salt("is_default");

    private static final long G_DELETED     = salt("deleted_gate");
    private static final long G_BIO         = salt("bio_gate");
    private static final long G_TRACKING    = salt("tracking_gate");

    private static final long N_WEIGHT      = salt("weight_grams");
    private static final long N_QUANTITY    = salt("quantity");
    private static final long N_QTY         = salt("qty");
    private static final long N_RATING      = salt("rating");
    private static final long N_POSTAL      = salt("postal_code");

    // Category values and their weights.
    private static final String[] METHODS = {"card", "cash", "transfer", "crypto"};
    private static final int[] METHODS_W = {70, 20, 8, 2};

    private static final String[] CURRENCIES = {"USD", "EUR", "RUB"};
    private static final int[] CURRENCIES_W = {60, 25, 15};

    private static final String[] STATUSES = {"new", "paid", "shipped", "done"};
    private static final int[] STATUSES_W = {10, 25, 25, 40};

    private static final String[] EVENT_TYPES =
            {"view", "click", "purchase", "logout", "error"};
    private static final int[] EVENT_W = {80, 10, 5, 3, 2};

    private static final String[] ENTITY_TYPES =
            {"user", "order", "product", "payment", "shipment"};
    private static final int[] ENTITY_W = {20, 30, 30, 10, 10};

    // Value dictionaries for text columns.
    private static final String[] CITIES = {
        "Москва", "Санкт-Петербург", "Новосибирск", "Екатеринбург", "Казань",
        "Нижний Новгород", "Челябинск", "Самара", "Омск", "Ростов-на-Дону",
        "Уфа", "Красноярск", "Воронеж", "Пермь", "Волгоград",
        "Краснодар", "Саратов", "Тюмень", "Тольятти", "Ижевск",
        "Барнаул", "Ульяновск", "Иркутск", "Хабаровск", "Ярославль",
        "Владивосток", "Махачкала", "Томск", "Оренбург", "Кемерово",
        "Новокузнецк", "Рязань", "Астрахань", "Набережные Челны", "Пенза",
        "Липецк", "Тула", "Киров", "Чебоксары", "Калининград",
        "Брянск", "Курск", "Иваново", "Магнитогорск", "Тверь",
        "Ставрополь", "Симферополь", "Белгород", "Архангельск", "Владимир",
        "Сочи", "Курган", "Смоленск", "Калуга", "Чита",
        "Орёл", "Волжский", "Череповец", "Владикавказ", "Мурманск",
        "Сургут", "Вологда", "Тамбов", "Стерлитамак", "Грозный",
        "Якутск", "Кострома", "Комсомольск-на-Амуре", "Петрозаводск", "Таганрог",
        "Нижневартовск", "Йошкар-Ола", "Братск", "Новороссийск", "Дзержинск",
        "Шахты", "Нальчик", "Орск", "Сыктывкар", "Нижнекамск"
    };

    private static final String[] STREET_PREFIXES = {
        "ул.", "пр-т", "пер.", "наб.", "пл.", "ш.", "б-р", "пр-д", "тр.", "аллея"
    };

    private static final String[] STREET_ROOTS = {
        "Ленина", "Гагарина", "Пушкина", "Лесная", "Садовая",
        "Школьная", "Молодёжная", "Центральная", "Заводская", "Полевая",
        "Северная", "Южная", "Восточная", "Западная", "Юбилейная",
        "Советская", "Первомайская", "Октябрьская", "Кирова", "Чехова",
        "Мира", "Победы", "Свободы", "Дружбы", "Пионерская",
        "Цветочная", "Берёзовая", "Дубовая", "Сосновая", "Рябиновая"
    };

    private static final String[] FIRST_NAMES = {
        "Александр", "Сергей", "Дмитрий", "Андрей", "Алексей",
        "Максим", "Иван", "Михаил", "Артём", "Никита",
        "Денис", "Евгений", "Антон", "Павел", "Илья",
        "Кирилл", "Роман", "Юрий", "Владимир", "Виктор",
        "Анна", "Мария", "Елена", "Ольга", "Наталья",
        "Татьяна", "Светлана", "Екатерина", "Юлия", "Ирина",
        "Дарья", "Алиса", "Полина", "Виктория", "Ксения",
        "Анастасия", "София", "Валерия", "Маргарита", "Вероника"
    };

    private static final String[] LAST_NAMES = {
        "Иванов", "Смирнов", "Кузнецов", "Попов", "Васильев",
        "Петров", "Соколов", "Михайлов", "Новиков", "Фёдоров",
        "Морозов", "Волков", "Алексеев", "Лебедев", "Семёнов",
        "Егоров", "Павлов", "Козлов", "Степанов", "Николаев",
        "Орлов", "Андреев", "Макаров", "Никитин", "Захаров",
        "Зайцев", "Соловьёв", "Борисов", "Яковлев", "Григорьев"
    };

    private ShopScenarios() {
    }

    private static LocalDateTime randomTime(long id, long salt) {
        long secs = Math.floorMod(stableHash(id, salt), TARGET_SECONDS);
        return BASE_DT.plusSeconds(secs);
    }

    private static int intMod(long id, long salt, int mod) {
        return (int) Math.floorMod(stableHash(id, salt), mod);
    }

    private static boolean oneIn(long id, long salt, int n) {
        return Math.floorMod(stableHash(id, salt), n) == 0;
    }

    private static String makeStreet(long id) {
        String prefix = pickFromArray(id, STREET_PREFIXES, TXT_STREET_PFX);
        String root = pickFromArray(id, STREET_ROOTS, TXT_STREET_ROOT);
        return prefix + " " + root + ", д. " + (intMod(id, TXT_HOUSE, 200) + 1);
    }

    private static String makeName(long id) {
        String first = pickFromArray(id, FIRST_NAMES, TXT_FIRST);
        String last = pickFromArray(id, LAST_NAMES, TXT_LAST);
        return first + " " + last;
    }

    public static TableScenario users(long n) {
        return Scenario.table("users", n)
                .col("id",              INT64,    id -> id)
                .col("email",           STRING,   id -> "user" + id + "@example.com")
                .col("name",            STRING,   ShopScenarios::makeName)
                .col("is_verified",     BOOL,     id -> intMod(id, B_VERIFIED, 100) < 67)
                .col("registered_date", DATE,     id ->
                        BASE_DATE.plusDays(intMod(id, T_REGISTERED, 1460)))
                .col("bio",             STRING,   id ->
                        oneIn(id, G_BIO, 5) ? null : "Bio of user " + id)
                .col("deleted_at",      DATETIME, id ->
                        oneIn(id, G_DELETED, 100) ? randomTime(id, T_DELETED) : null)
                .build();
    }

    public static TableScenario categories(long n) {
        return Scenario.table("categories", n)
                .col("id",        INT64,  id -> id)
                .col("name",      STRING, id -> "Category-" + id)
                .col("parent_id", INT64,  id -> id <= 10 ? 0L : ((id - 1) % 10) + 1)
                .build();
    }

    public static TableScenario products(long n, long categoryCount) {
        return Scenario.table("products", n)
                .col("id",           INT64,        id -> id)
                .col("category_id",  INT64,        id ->
                        zipfLikePick(id, categoryCount, ZIPF_ALPHA, FK_CATEGORY))
                .col("title",        STRING,       id -> "Product #" + id)
                .col("price",        DECIMAL_18_4, id ->
                        BigDecimal.valueOf((id % 10000) * 100 + 99, 2)
                                .setScale(4, RoundingMode.UNNECESSARY))
                .col("weight_grams", INT32,        id -> intMod(id, N_WEIGHT, 50000))
                .col("is_available", BOOL,         id -> !oneIn(id, B_AVAILABLE, 7))
                .partition(PartitionStyle.HASH_INT, "weight_grams")
                .build();
    }

    public static TableScenario inventory(long n, long productCount) {
        final long warehouseCount = Math.max(50L, n / 1000L);
        return Scenario.table("inventory", n)
                .col("id",           INT64,    id -> id)
                .col("product_id",   INT64,    id ->
                        zipfLikePick(id, productCount, ZIPF_ALPHA, FK_PRODUCT))
                .col("warehouse_id", INT32,    id -> intMod(id, FK_WAREHOUSE, (int) warehouseCount))
                .col("quantity",     INT32,    id -> intMod(id, N_QUANTITY, 10000))
                .col("updated_at",   DATETIME, id -> randomTime(id, T_UPDATE))
                .partition(PartitionStyle.HASH_INT, "warehouse_id")
                .build();
    }

    public static TableScenario orders(long n, long userCount) {
        return Scenario.table("orders", n)
                .col("id",        INT64,        id -> id)
                .col("user_id",   INT64,        id ->
                        zipfLikePick(id, userCount, ZIPF_ALPHA, FK_USER))
                .col("total",     DECIMAL_18_4, id ->
                        BigDecimal.valueOf(id * 13, 2)
                                .setScale(4, RoundingMode.UNNECESSARY))
                .col("currency",  STRING,       id ->
                        weighted(id, CURRENCIES, CURRENCIES_W, CAT_CURRENCY))
                .col("placed_at", DATETIME,     id -> randomTime(id, T_PLACED))
                .col("status",    STRING,       id ->
                        weighted(id, STATUSES, STATUSES_W, CAT_STATUS))
                .partition(PartitionStyle.RANGE_DATE, "placed_at")
                .build();
    }

    public static TableScenario orderItems(long n, long orderCount, long productCount) {
        return Scenario.table("order_items", n)
                .col("id",         INT64,        id -> id)
                .col("order_id",   INT64,        id ->
                        zipfLikePick(id, orderCount, ZIPF_ALPHA, FK_ORDER))
                .col("product_id", INT64,        id ->
                        zipfLikePick(id, productCount, ZIPF_ALPHA, FK_PRODUCT))
                .col("qty",        INT32,        id -> intMod(id, N_QTY, 10) + 1)
                .col("unit_price", DECIMAL_18_4, id ->
                        BigDecimal.valueOf((id % 5000) * 100 + 50, 2)
                                .setScale(4, RoundingMode.UNNECESSARY))
                .partition(PartitionStyle.RANGE_INT, "order_id")
                .build();
    }

    public static TableScenario payments(long n, long orderCount) {
        return Scenario.table("payments", n)
                .col("id",       INT64,        id -> id)
                .col("order_id", INT64,        id ->
                        zipfLikePick(id, orderCount, ZIPF_ALPHA, FK_ORDER))
                .col("amount",   DECIMAL_18_4, id ->
                        BigDecimal.valueOf(id * 11, 2)
                                .setScale(4, RoundingMode.UNNECESSARY))
                .col("method",   STRING,       id ->
                        weighted(id, METHODS, METHODS_W, CAT_METHOD))
                .col("paid_at",  DATETIME,     id -> randomTime(id, T_PAID))
                .partition(PartitionStyle.LIST_STRING, "method")
                .build();
    }

    public static TableScenario reviews(long n, long userCount, long productCount) {
        return Scenario.table("reviews", n)
                .col("id",          INT64,    id -> id)
                .col("product_id",  INT64,    id ->
                        zipfLikePick(id, productCount, ZIPF_ALPHA, FK_PRODUCT))
                .col("user_id",     INT64,    id ->
                        zipfLikePick(id, userCount, ZIPF_ALPHA, FK_USER))
                .col("rating",      INT32,    id -> intMod(id, N_RATING, 5) + 1)
                .col("review_text", STRING,   id -> "Отзыв №" + id)
                .col("reviewed_at", DATETIME, id -> randomTime(id, T_REVIEWED))
                .partition(PartitionStyle.RANGE_DATE, "reviewed_at")
                .build();
    }

    public static TableScenario addresses(long n, long userCount) {
        return Scenario.table("addresses", n)
                .col("id",          INT64,  id -> id)
                .col("user_id",     INT64,  id ->
                        zipfLikePick(id, userCount, ZIPF_ALPHA, FK_USER))
                .col("city",        STRING, id -> pickFromArray(id, CITIES, TXT_CITY))
                .col("street",      STRING, ShopScenarios::makeStreet)
                .col("postal_code", STRING, id ->
                        String.format("%06d", intMod(id, N_POSTAL, 200000)))
                .col("is_default",  BOOL,   id -> oneIn(id, B_DEFAULT, 3))
                .partition(PartitionStyle.HASH_INT, "user_id")
                .build();
    }

    public static TableScenario shipments(long n, long orderCount, long addressCount) {
        return Scenario.table("shipments", n)
                .col("id",            INT64,    id -> id)
                .col("order_id",      INT64,    id ->
                        zipfLikePick(id, orderCount, ZIPF_ALPHA, FK_ORDER))
                .col("address_id",    INT64,    id ->
                        zipfLikePick(id, addressCount, ZIPF_ALPHA, FK_ADDRESS))
                .col("shipped_at",    DATETIME, id -> randomTime(id, T_SHIPPED))
                .col("tracking_code", STRING,   id ->
                        oneIn(id, G_TRACKING, 4) ? null : "TRK-" + id)
                .partition(PartitionStyle.HASH_INT, "address_id")
                .build();
    }

    public static TableScenario productImages(long n, long productCount) {
        return Scenario.table("product_images", n)
                .col("id",         INT64,  id -> id)
                .col("product_id", INT64,  id ->
                        zipfLikePick(id, productCount, ZIPF_ALPHA, FK_PRODUCT))
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
                .col("entity_type", STRING,   id ->
                        weighted(id, ENTITY_TYPES, ENTITY_W, CAT_ENTITY))
                .col("entity_id",   INT64,    id ->
                        Math.floorMod(stableHash(id, FK_ORDER), Math.max(1L, n)) + 1)
                .col("action_type", STRING,   id ->
                        weighted(id, EVENT_TYPES, EVENT_W, CAT_EVENT))
                .col("ts",          DATETIME, id -> randomTime(id, T_TS))
                .partition(PartitionStyle.RANGE_DATE, "ts")
                .build();
    }

    public static List<TableScenario> all(long n) {
        long categoryCount = Math.max(10L, n / 1000L);
        long userCount     = Math.max(1L, n / 2L);
        long productCount  = Math.max(1L, n / 2L);
        long addressCount  = Math.max(1L, n / 2L);
        long reviewsCount  = Math.max(1L, n / 4L);
        long orderCount    = n;
        long inventoryN    = n;
        long paymentsCount = 5L * n / 4L;
        long shipmentsCnt  = n;
        long orderItemsCnt = 11L * n / 4L;
        long auditCount    = 11L * n / 4L;

        List<TableScenario> list = new ArrayList<>();
        list.add(users(userCount));
        list.add(categories(categoryCount));
        list.add(products(productCount, categoryCount));
        list.add(inventory(inventoryN, productCount));
        list.add(orders(orderCount, userCount));
        list.add(orderItems(orderItemsCnt, orderCount, productCount));
        list.add(payments(paymentsCount, orderCount));
        list.add(reviews(reviewsCount, userCount, productCount));
        list.add(addresses(addressCount, userCount));
        list.add(shipments(shipmentsCnt, orderCount, addressCount));
        list.add(productImages(BLOB_ROW_COUNT, productCount));
        list.add(auditLog(auditCount));
        return list;
    }

    public static List<TableScenario> uniform(int tables, long rowsPerTable) {
        List<TableScenario> list = new ArrayList<>(tables);
        for (int i = 0; i < tables; i++) {
            list.add(uniformTable("uniform_" + i, rowsPerTable));
        }
        return list;
    }

    private static TableScenario uniformTable(String name, long n) {
        final long userCount = Math.max(1L, n / 2L);
        return Scenario.table(name, n)
                .col("id",        INT64,        id -> id)
                .col("user_id",   INT64,        id ->
                        zipfLikePick(id, userCount, ZIPF_ALPHA, FK_USER))
                .col("total",     DECIMAL_18_4, id ->
                        BigDecimal.valueOf(id * 13, 2)
                                .setScale(4, RoundingMode.UNNECESSARY))
                .col("currency",  STRING,       id ->
                        weighted(id, CURRENCIES, CURRENCIES_W, CAT_CURRENCY))
                .col("placed_at", DATETIME,     id -> randomTime(id, T_PLACED))
                .col("status",    STRING,       id ->
                        weighted(id, STATUSES, STATUSES_W, CAT_STATUS))
                .build();
    }
}
