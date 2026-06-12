package tech.ydb.importer.integration.verification;

/**
 * Helpers that turn an id into a stable value.
 * The same id always gives the same result.
 */
public final class ScenarioRandom {

    private ScenarioRandom() {
    }

    /** A salt for a column, taken from its name. */
    public static long salt(String name) {
        long h = stableHash(name.length());
        for (int i = 0; i < name.length(); i++) {
            h = stableHash(h ^ name.charAt(i));
        }
        return h;
    }

    /** Spreads sequential ids evenly across the range. */
    public static long stableHash(long id) {
        long h = id;
        h ^= h >>> 33;
        h *= 0xff51afd7ed558ccdL;
        h ^= h >>> 33;
        h *= 0xc4ceb9fe1a85ec53L;
        h ^= h >>> 33;
        return h;
    }

    /** Different columns of the same row stay independent. */
    public static long stableHash(long id, long salt) {
        return stableHash(id ^ salt);
    }

    public static double uniform(long id, long salt) {
        long bits = stableHash(id, salt) & 0x3FFFFFFFL;
        return (bits + 1.0) / (1L << 30);
    }

    /** Picks a number from 1 to n, smaller ones more often. */
    public static long zipfLikePick(long id, long n, double alpha, long salt) {
        if (n <= 1) {
            return 1L;
        }
        return 1L + (long) ((n - 1) * Math.pow(uniform(id, salt), alpha));
    }

    public static <T> T weighted(long id, T[] values, int[] weights, long salt) {
        int total = 0;
        for (int w : weights) {
            total += w;
        }
        int x = (int) Math.floorMod(stableHash(id, salt), total);
        int acc = 0;
        for (int i = 0; i < values.length; i++) {
            acc += weights[i];
            if (x < acc) {
                return values[i];
            }
        }
        return values[values.length - 1];
    }

    public static <T> T pickFromArray(long id, T[] dict, long salt) {
        int idx = (int) Math.floorMod(stableHash(id, salt), dict.length);
        return dict[idx];
    }
}
