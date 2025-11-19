package hello1.koddata.concurrent.cluster;

import hello1.koddata.net.NetUtils;
import hello1.koddata.net.NodeStatus;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class ResourceLock {

    private static final int RESPONSE_TIMEOUT_MS = 1000;

    private final String clusterKey;

    private final AtomicLong lockingProcess = new AtomicLong(-1);
    private final Queue<Long> acquireQueue = new LinkedList<>();
    private volatile boolean isFair = false;

    private final ConcurrentMap<Long, LockRequestTracker> pendingLockRequests = new ConcurrentHashMap<>();

    private static class LockRequestTracker {
        final CompletableFuture<Boolean> lockFuture = new CompletableFuture<>();
        final CountDownLatch grantLatch;
        volatile boolean denied = false;

        LockRequestTracker(int peerCount) {
            this.grantLatch = new CountDownLatch(peerCount);
        }
    }


    public ResourceLock(String clusterKey) {
        this.clusterKey = clusterKey;
    }


    public boolean lock(long processId, Set<NodeStatus> peers) throws InterruptedException {
        synchronized (this) {
            if (lockingProcess.get() == -1 || lockingProcess.get() == processId) {
                lockingProcess.set(processId);
                return true;
            }

            if (isFair && !acquireQueue.contains(processId)) {
                acquireQueue.add(processId);
            }
        }

        List<NodeStatus> activePeers = peers.stream()
                .filter(NodeStatus::isAvailable)
                .collect(Collectors.toList());

        if (activePeers.isEmpty()) {
            synchronized (this) {
                if (lockingProcess.get() == -1) {
                    lockingProcess.set(processId);
                    return true;
                }
            }
            return false;
        }

        LockRequestTracker tracker = new LockRequestTracker(activePeers.size());
        pendingLockRequests.put(processId, tracker);

        broadcastLockRequest(processId, activePeers);

        try {
            tracker.grantLatch.await(RESPONSE_TIMEOUT_MS, TimeUnit.MILLISECONDS);

            boolean granted = tracker.lockFuture.getNow(false);

            if (granted && !tracker.denied) {
                synchronized (this) {
                    lockingProcess.set(processId);
                    if (isFair) {
                        acquireQueue.remove(processId);
                    }
                }
                return true;
            } else {
                return false;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            pendingLockRequests.remove(processId);
            throw e;
        } finally {
            pendingLockRequests.remove(processId);
        }
    }


    public synchronized boolean tryLock(long processId, Set<NodeStatus> peers) {
        if (lockingProcess.get() == -1) {
            lockingProcess.set(processId);
            broadcastLockRequest(processId, peers);
            return true;
        }
        if (lockingProcess.get() == processId) {
            return true;
        }
        return false;
    }

    public synchronized void unlock(long processId, Set<NodeStatus> peers) {
        if (lockingProcess.get() != processId) {
            throw new IllegalMonitorStateException("Process " + processId + " does not hold the lock");
        }
        lockingProcess.set(-1);
        broadcastUnlock(processId, peers);
        notifyAll();
    }


    private void broadcastLockRequest(long processId, Collection<NodeStatus> peers) {
        for (NodeStatus node : peers) {
            if (!node.isAvailable()) continue;
            try {
                NetUtils.sendMessage(node.getChannel(),
                        NetUtils.OPCODE_LOCK_REQUEST,
                        clusterKey,
                        String.valueOf(processId));
            } catch (IOException e) {
                node.setAvailable(false);
            }
        }
    }


    private void broadcastUnlock(long processId, Collection<NodeStatus> peers) {
        for (NodeStatus node : peers) {
            if (!node.isAvailable()) continue;
            try {
                NetUtils.sendMessage(node.getChannel(),
                        NetUtils.OPCODE_UNLOCK,
                        clusterKey,
                        String.valueOf(processId));
            } catch (IOException e) {
                node.setAvailable(false);
            }
        }
    }

    public synchronized void handleLockRequest(long processId, SocketChannel channel) {
        boolean grantLock;

        if (lockingProcess.get() == -1) {
            lockingProcess.set(processId);
            grantLock = true;
        } else if (lockingProcess.get() == processId) {
            grantLock = true;
        } else {
            grantLock = false;
        }

        try {
            NetUtils.sendMessage(channel,
                    grantLock ? NetUtils.OPCODE_LOCK_GRANTED : NetUtils.OPCODE_LOCK_DENIED,
                    clusterKey,
                    String.valueOf(processId));
        } catch (IOException e) {
        }
    }

    public void handleLockGranted(long processId) {
        LockRequestTracker tracker = pendingLockRequests.get(processId);
        if (tracker == null) return;

        tracker.grantLatch.countDown();

        if (tracker.grantLatch.getCount() == 0) {
            tracker.lockFuture.complete(true);
        }
    }

    public void handleLockDenied(long processId) {
        LockRequestTracker tracker = pendingLockRequests.get(processId);
        if (tracker == null) return;

        tracker.denied = true;
        tracker.lockFuture.complete(false);

        while (tracker.grantLatch.getCount() > 0) {
            tracker.grantLatch.countDown();
        }
    }


    public synchronized void handleUnlock(long processId) {
        if (lockingProcess.get() == processId) {
            lockingProcess.set(-1);
            notifyAll();
        }
    }

    public synchronized boolean isLocked() {
        return lockingProcess.get() != -1;
    }

    public synchronized long getLockingProcess() {
        return lockingProcess.get();
    }

    public synchronized boolean isFair() {
        return isFair;
    }

    public synchronized void setFair(boolean isFair) {
        this.isFair = isFair;
    }

    public synchronized void reset() {
        lockingProcess.set(-1);
        acquireQueue.clear();
        notifyAll();
    }

    public String getClusterKey() {
        return clusterKey;
    }
}