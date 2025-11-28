package hello1.koddata.utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ByteUtils {

    public static byte[][] splitBytes(byte[] bytes, byte delimiter) {
        if (bytes == null) return new byte[0][];

        List<byte[]> parts = new ArrayList<>();
        int start = 0;

        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] == delimiter) {
                byte[] segment = Arrays.copyOfRange(bytes, start, i);
                parts.add(segment);
                start = i + 1;
            }
        }

        byte[] segment = Arrays.copyOfRange(bytes, start, bytes.length);
        parts.add(segment);

        return parts.toArray(new byte[parts.size()][]);
    }


    public static int bytesToInt(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.put(new byte[4 - bytes.length]);
        buffer.put(bytes);
        buffer.flip();
        return buffer.getInt();
    }

    public static byte[] intToBytes(int value) {
        return new byte[] {
                (byte) (value >> 24),
                (byte) (value >> 16),
                (byte) (value >> 8),
                (byte) value
        };
    }

}
