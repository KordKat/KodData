package hello1.koddata.concurrent;


import java.util.concurrent.TimeUnit;

public class ClusterAwait {

    private String clusterKey;
    private int initialCount;
    private volatile int currentCount;
    private boolean autoReset;
    private long timeoutMillis;

    public ClusterAwait(String clusterKey, int count) {}
    public ClusterAwait(String clusterKey, int count, boolean autoReset) {}
    public ClusterAwait(String clusterKey, int count, boolean autoReset, long timeoutMillis) {}

    public void await() throws InterruptedException {}
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException { return false; }
    public void countDown() {}
    public int getCount() { return 0; }

    public void syncWithCluster() {}
    public void notifyClusterCountDown() {}
    public void reset() {}

    public boolean isZero() { return false; }
    public String getClusterKey() { return null; }
    public boolean isAutoReset() { return false; }
    public void setAutoReset(boolean autoReset) {}

}
