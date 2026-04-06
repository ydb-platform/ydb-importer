package tech.ydb.importer.target;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalTime;
import java.util.Base64;

import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.Value;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class SynthKey {

    private static final byte[] SEPARATOR = new byte[]{0x02};
    private final MessageDigest digest;
    private final Base64.Encoder base64Encoder;

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
        updateSeparator();
    }

    public void hashBool(boolean v) {
        ByteBuffer buf = ByteBuffer.allocate(1);
        buf.put((byte) (v ? 1 : 0));
        update(buf);
        updateSeparator();
    }

    public void hashInt(int v) {
        ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES);
        buf.putInt(v);
        update(buf);
        updateSeparator();
    }

    public void hashLong(long v) {
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES);
        buf.putLong(v);
        update(buf);
        updateSeparator();
    }

    public void hashFloat(float v) {
        ByteBuffer buf = ByteBuffer.allocate(Float.BYTES);
        buf.putFloat(v);
        update(buf);
        updateSeparator();
    }

    public void hashDouble(double v) {
        ByteBuffer buf = ByteBuffer.allocate(Double.BYTES);
        buf.putDouble(v);
        update(buf);
        updateSeparator();
    }

    public void hashString(String v) {
        update(v.getBytes());
        updateSeparator();
    }

    public void hashBytes(byte[] v) {
        update(v);
        updateSeparator();
    }

    public void hashBigDecimal(BigDecimal v) {
        byte[] bytes = v.toBigInteger().toByteArray();
        ByteBuffer buf = ByteBuffer.allocate(bytes.length + Integer.BYTES);
        buf.putInt(v.scale());
        buf.put(bytes);
        update(buf);
        updateSeparator();
    }

    public void hashDate(java.sql.Date v) {
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES);
        buf.putLong(v.getTime());
        update(buf);
        updateSeparator();
    }

    public void hashTime(java.sql.Time v) {
        LocalTime lt = v.toLocalTime();
        ByteBuffer buf = ByteBuffer.allocate(4 * Integer.BYTES);
        buf.putInt(lt.getHour());
        buf.putInt(lt.getMinute());
        buf.putInt(lt.getSecond());
        buf.putInt(lt.getNano());
        update(buf);
        updateSeparator();
    }

    public void hashTimestamp(java.sql.Timestamp v) {
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
        buf.putLong(v.getTime());
        buf.putInt(v.getNanos());
        update(buf);
        updateSeparator();
    }

    public void hashUuidText(String v) {
        update(v.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        updateSeparator();
    }

    public String buildString() {
        byte[] sign = digest.digest();
        return base64Encoder.encodeToString(sign);
    }

    public Value<?> build() {
        return PrimitiveValue.newText(buildString());
    }
}
