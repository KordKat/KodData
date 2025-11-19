package hello1.koddata.concurrent.cluster;

import hello1.koddata.net.NetUtils;
import hello1.koddata.net.NodeStatus;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class ClusterLockService {



    private static final long LOCK_WAIT_TIMEOUT_MS = 2000;

    private static final ConcurrentHashMap<String, ClusterLock> locks = new ConcurrentHashMap<>();

    public static ClusterLock getLock(String name) {
        return locks.computeIfAbsent(name, ClusterLock::new);
    }

    public static void startService() {
        Thread.ofPlatform().start(() -> {
            while (true) {
                try {
                    ClusterMessage msg = ClusterMessageBus.take();

                    Thread.ofVirtual().start(() -> {
                        try {
                            process(msg);
                        } catch (Throwable t) {
                            t.printStackTrace();
                        }
                    });
                } catch (InterruptedException ignored) {}
            }
        });
    }

    private static void process(ClusterMessage msg) {
        switch (msg.opcode) {
            case NetUtils.OPCODE_LOCK_REQUEST -> handleLockRequest(msg);
            case NetUtils.OPCODE_UNLOCK -> handleUnlock(msg);
        }
    }

    private static void handleLockRequest(ClusterMessage msg) {
        ClusterLock lock = getLock(msg.resourceName);

        lock.lock();

        try {
            NetUtils.sendMessage(msg.channel, NetUtils.OPCODE_LOCK_OK, msg.resourceName);
        } catch (IOException ignored) {}
    }

    private static void handleUnlock(ClusterMessage msg) {
        ClusterLock lock = getLock(msg.resourceName);
        lock.unlock();
    }

    public static boolean acquireLock(String resourceName, Set<NodeStatus> peers) {
        int availablePeers = (int) peers.stream().filter(NodeStatus::isAvailable).count();
        if (availablePeers == 0) return true;

        ClusterLockWaiter waiter = ClusterLockWaiter.create(resourceName, availablePeers);

        for (NodeStatus ns : peers) {
            if (!ns.isAvailable()) continue;

            try {
                NetUtils.sendMessage(ns.getChannel(), NetUtils.OPCODE_LOCK_REQUEST, resourceName);
            } catch (IOException e) {
                ns.setAvailable(false);
            }
        }

        try {
            waiter.waitForAll(LOCK_WAIT_TIMEOUT_MS);
        } catch (InterruptedException ignored) {}

        return waiter.latch.getCount() == 0;
    }

    public static void releaseLock(String resourceName, Set<NodeStatus> peers) {
        for (NodeStatus ns : peers) {
            if (!ns.isAvailable()) continue;

            try {
                NetUtils.sendMessage(ns.getChannel(), NetUtils.OPCODE_UNLOCK, resourceName);
            } catch (IOException e) {
                ns.setAvailable(false);
            }
        }

        getLock(resourceName).unlock();
    }

    public static void handleLockOk(String resourceName) {
        ClusterLockWaiter.signal(resourceName);
    }
}
