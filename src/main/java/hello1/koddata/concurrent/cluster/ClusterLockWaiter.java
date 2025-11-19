package hello1.koddata.concurrent.cluster;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ClusterLockWaiter {

    private static final ConcurrentMap<String, ClusterLockWaiter> waiters = new ConcurrentHashMap<>();

    final CountDownLatch latch;

    private ClusterLockWaiter(int expectedResponses) {
        this.latch = new CountDownLatch(expectedResponses);
    }

    public static ClusterLockWaiter create(String resourceName, int expectedResponses) {
        ClusterLockWaiter waiter = new ClusterLockWaiter(expectedResponses);
        waiters.put(resourceName, waiter);
        return waiter;
    }

    public static void signal(String resourceName) {
        ClusterLockWaiter waiter = waiters.get(resourceName);
        if (waiter != null) {
            waiter.latch.countDown();
        }
    }

    public void waitForAll(long timeoutMillis) throws InterruptedException {
        latch.await(timeoutMillis, java.util.concurrent.TimeUnit.MILLISECONDS);
        waiters.remove(this);
    }
}
