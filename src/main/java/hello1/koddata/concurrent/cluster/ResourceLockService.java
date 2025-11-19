package hello1.koddata.concurrent.cluster;

import hello1.koddata.net.NetUtils;
import hello1.koddata.net.NodeStatus;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ResourceLockService {

    private static final Map<String, ResourceLock> locks = new ConcurrentHashMap<>();
    private static final BlockingQueue<ResourceLockClusterMessage> messageQueue = new LinkedBlockingQueue<>();
    private static final AtomicBoolean running = new AtomicBoolean(false);
    private static Thread serviceThread;
    private static Set<NodeStatus> peers;

    private ResourceLockService() {
    }

    public static void initialize(Set<NodeStatus> clusterPeers) {
        peers = clusterPeers;
    }

    public static void enqueueMessage(ResourceLockClusterMessage msg) {
        messageQueue.offer(msg);
    }

    public static void start() {
        if (running.compareAndSet(false, true)) {
            serviceThread = new Thread(ResourceLockService::processLoop, "ResourceLockService");
            serviceThread.start();
        }
    }

    public static void stop() {
        running.set(false);
        if (serviceThread != null) {
            serviceThread.interrupt();
        }
    }

    public static ResourceLock getLock(String resourceKey) {
        return locks.computeIfAbsent(resourceKey, ResourceLock::new);
    }

    private static void processLoop() {
        ThreadFactory vtFactory = Thread.ofVirtual().factory();

        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                ResourceLockClusterMessage msg = messageQueue.take();
                vtFactory.newThread(() -> handleMessage(msg)).start();

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static void handleMessage(ResourceLockClusterMessage msg) {
        byte opcode = msg.opcode();
        String resourceKey = msg.resourceKey();
        long processId = msg.processId();
        SocketChannel channel = msg.channel();

        ResourceLock lock = getLock(resourceKey);

        try {
            switch (opcode) {
                case NetUtils.OPCODE_LOCK_REQUEST -> lock.handleLockRequest(processId, channel);
                case NetUtils.OPCODE_LOCK_GRANTED -> lock.handleLockGranted(processId);
                case NetUtils.OPCODE_LOCK_DENIED -> lock.handleLockDenied(processId);
                case NetUtils.OPCODE_UNLOCK -> lock.handleUnlock(processId);
                default -> {
                    System.err.println("Unknown opcode " + opcode);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            if (channel != null) {
                markPeerUnavailable(channel);
            }
        }
    }

    private static void markPeerUnavailable(SocketChannel channel) {
        if (peers == null) return;
        for (NodeStatus ns : peers) {
            if (channel.equals(ns.getChannel())) {
                ns.setAvailable(false);
                break;
            }
        }
    }

    public static boolean lock(String resourceKey, long processId) throws InterruptedException {
        ResourceLock lock = getLock(resourceKey);
        if (peers == null) return false;
        return lock.lock(processId, peers);
    }

    public static boolean tryLock(String resourceKey, long processId) {
        ResourceLock lock = getLock(resourceKey);
        if (peers == null) return false;
        return lock.tryLock(processId, peers);
    }

    public static void unlock(String resourceKey, long processId) {
        ResourceLock lock = getLock(resourceKey);
        if (peers == null) return;
        lock.unlock(processId, peers);
    }
}