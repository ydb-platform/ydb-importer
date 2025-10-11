package tech.ydb.importer.target;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
        digest.update(buffer);
    }

    public void updateSeparator() {
        digest.update(SEPARATOR);
    }

    public Value<?> build() {
        byte[] sign = digest.digest();
        return PrimitiveValue.newText(base64Encoder.encodeToString(sign));
    }
}
