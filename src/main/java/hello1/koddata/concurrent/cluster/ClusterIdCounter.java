package hello1.koddata.concurrent.cluster;

import hello1.koddata.net.NetUtils;
import hello1.koddata.net.NodeStatus;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class ClusterIdCounter {

    private static final ConcurrentMap<String, ClusterIdCounter> clusterIdCounters = new ConcurrentHashMap<>();

    private final AtomicLong idCounter = new AtomicLong(0);
    private final String name;
    private final Set<NodeStatus> peers;

    private ClusterIdCounter(Class<?> clazz, Set<NodeStatus> peers) {
        this.name = clazz.getName();
        this.peers = peers;
    }

    public static ClusterIdCounter getCounter(Class<?> clazz, Set<NodeStatus> peers) {
        return clusterIdCounters.computeIfAbsent(clazz.getName(),
                k -> new ClusterIdCounter(clazz, peers));
    }

    public void setCount(long newCount) {
        idCounter.set(newCount);
    }

    public long count() {
        if (peers == null || peers.isEmpty()) {
            return idCounter.incrementAndGet();
        }

        boolean locked = ClusterLockService.acquireLock(name, peers);

        if (!locked) {
            return -1;
        }

        try {
            long max = idCounter.get();

            for (NodeStatus ns : peers) {
                if (!ns.isAvailable()) continue;

                try {
                    long peerCount = NetUtils.requestCount(ns.getChannel(), name);
                    if (peerCount > max) max = peerCount;
                } catch (IOException e) {
                    ns.setAvailable(false);
                }
            }

            long next = max + 1;
            idCounter.set(next);

            return next;

        } finally {
            ClusterLockService.releaseLock(name, peers);
        }
    }

    public void handleCountRequest(SocketChannel channel) {
        try {
            NetUtils.sendMessage(channel, NetUtils.OPCODE_COUNT_RESPONSE, name, String.valueOf(idCounter.get()));
        } catch (IOException ignored) {}
    }

    public String getName() {
        return name;
    }
}
