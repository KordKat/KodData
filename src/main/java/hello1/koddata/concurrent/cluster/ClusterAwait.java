package hello1.koddata.concurrent.cluster;

import hello1.koddata.net.NetUtils;
import hello1.koddata.net.NodeStatus;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Distributed countdown latch that syncs countdown state across a cluster.
 */
public class ClusterAwait {

    private final String clusterKey;
    private final int initialCount;
    private volatile int currentCount;
    private volatile boolean autoReset;
    private volatile long timeoutMillis;

    private final Set<NodeStatus> peers;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition zeroCondition = lock.newCondition();

    private static final long RESPONSE_TIMEOUT_MS = 500;

    private final ConcurrentMap<String, CompletableFuture<Integer>> pendingResponses = new ConcurrentHashMap<>();

    private final BlockingQueue<ClusterMessage> incomingMessages = new LinkedBlockingQueue<>();

    private final ExecutorService executor;

    public ClusterAwait(String clusterKey, int count, Set<NodeStatus> peers) {
        this(clusterKey, count, false, 0L, peers);
    }

    public ClusterAwait(String clusterKey, int count, boolean autoReset, long timeoutMillis, Set<NodeStatus> peers) {
        if (count < 0) throw new IllegalArgumentException("count cannot be negative");
        this.clusterKey = clusterKey;
        this.initialCount = count;
        this.currentCount = count;
        this.autoReset = autoReset;
        this.timeoutMillis = timeoutMillis;
        this.peers = peers;

        this.executor = Executors.newSingleThreadExecutor(Thread.ofVirtual().factory());
        startMessageHandler();
    }

    private void startMessageHandler() {
        executor.submit(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    ClusterMessage msg = incomingMessages.take();
                    handleMessage(msg);
                }
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        });
    }

    public void receiveMessage(ClusterMessage msg) {
        incomingMessages.offer(msg);
    }

    private void handleMessage(ClusterMessage msg) {
        switch (msg.opcode) {
            case NetUtils.OPCODE_CLUSTERAWAIT_COUNTDOWN:
                handleCountDownRequest(msg.channel, msg.resourceName);
                break;

            case NetUtils.OPCODE_CLUSTERAWAIT_COUNTDOWN_OK:
                // We can optionally handle ACK here if needed
                break;

            case NetUtils.OPCODE_CLUSTERAWAIT_GETCOUNT:
                handleGetCountRequest(msg.channel, msg.resourceName);
                break;

            case NetUtils.OPCODE_CLUSTERAWAIT_GETCOUNT_RESPONSE:
                try {
                    int count = Integer.parseInt(msg.additionalData);
                    handleGetCountResponse(msg.channel, msg.resourceName, count);
                } catch (NumberFormatException ignored) {}
                break;

            default:
                // Unknown opcode - ignore
                break;
        }
    }

    public void await() throws InterruptedException {
        lock.lock();
        try {
            if (timeoutMillis <= 0) {
                while (currentCount > 0) {
                    zeroCondition.await();
                }
            } else {
                long nanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
                while (currentCount > 0) {
                    if (nanos <= 0L) break;
                    nanos = zeroCondition.awaitNanos(nanos);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        lock.lock();
        try {
            long nanos = unit.toNanos(timeout);
            while (currentCount > 0) {
                if (nanos <= 0L) return false;
                nanos = zeroCondition.awaitNanos(nanos);
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    public void countDown() {
        lock.lock();
        try {
            if (currentCount == 0) return;
            currentCount--;
            if (currentCount == 0) zeroCondition.signalAll();
        } finally {
            lock.unlock();
        }
        notifyClusterCountDown();
    }

    public int getCount() {
        return currentCount;
    }

    public void syncWithCluster() {
        if (peers == null || peers.isEmpty()) return;

        pendingResponses.clear();

        CountDownLatch latch = new CountDownLatch(peers.size());

        for (NodeStatus peer : peers) {
            if (!peer.isAvailable()) {
                latch.countDown();
                continue;
            }

            CompletableFuture<Integer> future = new CompletableFuture<>();
            pendingResponses.put(peer.getChannel().toString(), future);

            try {
                NetUtils.sendMessage(peer.getChannel(), NetUtils.OPCODE_CLUSTERAWAIT_GETCOUNT, clusterKey);
            } catch (IOException e) {
                peer.setAvailable(false);
                latch.countDown();
                continue;
            }

            future.whenComplete((count, ex) -> latch.countDown());
        }

        try {
            latch.await(RESPONSE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignored) {
        }

        int minCount = currentCount;
        for (CompletableFuture<Integer> future : pendingResponses.values()) {
            int val = future.getNow(currentCount);
            if (val < minCount) minCount = val;
        }

        lock.lock();
        try {
            currentCount = minCount;
            if (currentCount == 0) zeroCondition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private void notifyClusterCountDown() {
        if (peers == null || peers.isEmpty()) return;

        for (NodeStatus peer : peers) {
            if (!peer.isAvailable()) continue;

            try {
                NetUtils.sendMessage(peer.getChannel(), NetUtils.OPCODE_CLUSTERAWAIT_COUNTDOWN, clusterKey);
            } catch (IOException e) {
                peer.setAvailable(false);
            }
        }
    }

    private void handleCountDownRequest(SocketChannel channel, String resourceName) {
        if (!clusterKey.equals(resourceName)) return;

        lock.lock();
        try {
            if (currentCount == 0) return;
            currentCount--;
            if (currentCount == 0) zeroCondition.signalAll();
        } finally {
            lock.unlock();
        }

        try {
            NetUtils.sendMessage(channel, NetUtils.OPCODE_CLUSTERAWAIT_COUNTDOWN_OK, clusterKey);
        } catch (IOException ignored) {}
    }

    private void handleGetCountRequest(SocketChannel channel, String resourceName) {
        if (!clusterKey.equals(resourceName)) return;

        int countToSend;
        lock.lock();
        try {
            countToSend = currentCount;
        } finally {
            lock.unlock();
        }

        try {
            NetUtils.sendMessage(channel, NetUtils.OPCODE_CLUSTERAWAIT_GETCOUNT_RESPONSE, clusterKey, String.valueOf(countToSend));
        } catch (IOException ignored) {}
    }

    private void handleGetCountResponse(SocketChannel channel, String resourceName, int count) {
        if (!clusterKey.equals(resourceName)) return;

        CompletableFuture<Integer> future = pendingResponses.remove(channel.toString());
        if (future != null) {
            future.complete(count);
        }
    }

    public void reset() {
        lock.lock();
        try {
            currentCount = initialCount;
            zeroCondition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public boolean isZero() {
        return currentCount == 0;
    }

    public String getClusterKey() {
        return clusterKey;
    }

    public boolean isAutoReset() {
        return autoReset;
    }

    public void setAutoReset(boolean autoReset) {
        this.autoReset = autoReset;
    }

    public void shutdown() {
        executor.shutdownNow();
    }

    public static class ClusterMessage {
        public final SocketChannel channel;
        public final int opcode;
        public final String resourceName;
        public final String additionalData; // Optional extra data

        public ClusterMessage(SocketChannel channel, int opcode, String resourceName, String additionalData) {
            this.channel = channel;
            this.opcode = opcode;
            this.resourceName = resourceName;
            this.additionalData = additionalData;
        }

        public ClusterMessage(SocketChannel channel, int opcode, String resourceName) {
            this(channel, opcode, resourceName, null);
        }
    }
}
