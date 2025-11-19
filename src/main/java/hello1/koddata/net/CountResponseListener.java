package hello1.koddata.net;

import java.nio.channels.SocketChannel;
import java.util.concurrent.*;

public class CountResponseListener {

    private static final ConcurrentMap<String, CompletableFuture<Long>> pendingCounts = new ConcurrentHashMap<>();

    public static CompletableFuture<Long> waitForCountResponse(SocketChannel channel, String resourceName) {
        String key = generateKey(channel, resourceName);
        CompletableFuture<Long> future = new CompletableFuture<>();
        pendingCounts.put(key, future);
        return future;
    }

    public static void onCountResponseReceived(SocketChannel channel, String resourceName, long value) {
        String key = generateKey(channel, resourceName);
        CompletableFuture<Long> future = pendingCounts.remove(key);
        if (future != null) {
            future.complete(value);
        }
    }

    private static String generateKey(SocketChannel channel, String resourceName) {
        return channel.hashCode() + ":" + resourceName;
    }
}

