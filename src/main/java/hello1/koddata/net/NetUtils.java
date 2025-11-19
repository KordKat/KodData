package hello1.koddata.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class NetUtils {

    public static final byte OPCODE_HEARTBEAT = 0x01;
    public static final byte OPCODE_STATUS = 0x02;
    public static final byte OPCODE_SHUTDOWN = 0x03;
    public static final byte OPCODE_STARTUP = 0x04;
    public static final byte OPCODE_TRANSFER = 0x05;
    public static final byte OPCODE_NEWTASK = 0x06;
    public static final byte OPCODE_COUNT = 0x07;
    public static final byte OPCODE_COUNT_RESPONSE = 0x08;
    public static final byte OPCODE_LOCK_REQUEST = 0x09;
    public static final byte OPCODE_LOCK_OK = 0x10;
    public static final byte OPCODE_UNLOCK = 0x11;

    public static final byte OPCODE_CLUSTERAWAIT_COUNTDOWN = 0x12;
    public static final byte OPCODE_CLUSTERAWAIT_COUNTDOWN_OK = 0x13;
    public static final byte OPCODE_CLUSTERAWAIT_GETCOUNT = 0x14;
    public static final byte OPCODE_CLUSTERAWAIT_GETCOUNT_RESPONSE = 0x15;

    public static final byte OPCODE_LOCK_GRANTED = 0x16;
    public static final byte OPCODE_LOCK_DENIED = 0x17;

    public static final byte OPCODE_INFO_DATA_PORT = 0x18;

    public static final byte OPCODE_DATATRANSFER = 0x19;

    public static void sendMessage(SocketChannel ch, byte[] data) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(data);
        buf.flip();
        ch.write(buf);
    }

    public static void sendMessage(SocketChannel ch, byte opcode) throws IOException {
        sendMessage(ch, new byte[]{opcode, 0});
    }

    public static void sendMessage(SocketChannel ch, byte opcode, String... msg) throws IOException {
        int totalSize = 1;

        byte[][] encoded = new byte[msg.length][];
        for (int i = 0; i < msg.length; i++) {
            encoded[i] = msg[i].getBytes(StandardCharsets.UTF_8);
            totalSize += 4 + encoded[i].length;
        }

        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.put(opcode);

        for (byte[] arr : encoded) {
            buffer.putInt(arr.length);
            buffer.put(arr);
        }

        buffer.flip();
        while (buffer.hasRemaining()) {
            ch.write(buffer);
        }
    }

    public static long requestCount(SocketChannel channel, String resourceName) throws IOException {
        sendMessage(channel, OPCODE_COUNT, resourceName);


        CompletableFuture<Long> future = CountResponseListener.waitForCountResponse(channel, resourceName);

        try {
            return future.get(500, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new IOException("Timeout waiting count response", e);
        }
    }

}
