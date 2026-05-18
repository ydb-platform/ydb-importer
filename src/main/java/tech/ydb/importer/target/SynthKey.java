package tech.ydb.importer.target;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalTime;
import java.util.Base64;

import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.Value;

/**
 * Builder of synthetic primary keys for imported rows.
 *
 * @author Aleksandr Gorshenin
 */
public class SynthKey {

    private static final byte[] SEPARATOR = new byte[]{0x02};
    private final MessageDigest digest;
    private final Base64.Encoder base64Encoder;
    private final ByteBuffer scratch = ByteBuffer.allocate(16);

    public SynthKey() throws NoSuchAlgorithmException {
        this.digest = MessageDigest.getInstance("SHA-256");
        this.base64Encoder = Base64.getUrlEncoder().withoutPadding();
    }

    public void update(byte[] bytes) {
        digest.update(bytes);
    }

    public void update(ByteBuffer buffer) {
        buffer.flip();
        digest.update(buffer);
    }

    public void updateSeparator() {
        digest.update(SEPARATOR);
    }

    public void hashNull() {
        digest.update(SEPARATOR);
    }

    public void hashBool(boolean v) {
        scratch.clear();
        scratch.put((byte) (v ? 1 : 0));
        scratch.flip();
        digest.update(scratch);
        digest.update(SEPARATOR);
    }

    public void hashInt(int v) {
        scratch.clear();
        scratch.putInt(v);
        scratch.flip();
        digest.update(scratch);
        digest.update(SEPARATOR);
    }

    public void hashLong(long v) {
        scratch.clear();
        scratch.putLong(v);
        scratch.flip();
        digest.update(scratch);
        digest.update(SEPARATOR);
    }

    public void hashFloat(float v) {
        scratch.clear();
        scratch.putFloat(v);
        scratch.flip();
        digest.update(scratch);
        digest.update(SEPARATOR);
    }

    public void hashDouble(double v) {
        scratch.clear();
        scratch.putDouble(v);
        scratch.flip();
        digest.update(scratch);
        digest.update(SEPARATOR);
    }

    public void hashString(String v) {
        digest.update(v.getBytes(StandardCharsets.UTF_8));
        digest.update(SEPARATOR);
    }

    public void hashBytes(byte[] v) {
        digest.update(v);
        digest.update(SEPARATOR);
    }

    public void hashBigDecimal(BigDecimal v) {
        scratch.clear();
        scratch.putInt(v.scale());
        scratch.flip();
        digest.update(scratch);
        digest.update(v.toBigInteger().toByteArray());
        digest.update(SEPARATOR);
    }

    public void hashDate(Date v) {
        scratch.clear();
        scratch.putLong(v.getTime());
        scratch.flip();
        digest.update(scratch);
        digest.update(SEPARATOR);
    }

    public void hashTime(Time v) {
        LocalTime lt = v.toLocalTime();
        scratch.clear();
        scratch.putInt(lt.getHour());
        scratch.putInt(lt.getMinute());
        scratch.putInt(lt.getSecond());
        scratch.putInt(lt.getNano());
        scratch.flip();
        digest.update(scratch);
        digest.update(SEPARATOR);
    }

    public void hashTimestamp(Timestamp v) {
        scratch.clear();
        scratch.putLong(v.getTime());
        scratch.putInt(v.getNanos());
        scratch.flip();
        digest.update(scratch);
        digest.update(SEPARATOR);
    }

    public Value<?> build() {
        byte[] sign = digest.digest();
        return PrimitiveValue.newText(base64Encoder.encodeToString(sign));
    }
}
