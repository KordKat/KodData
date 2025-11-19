package hello1.koddata.concurrent.cluster;

import java.util.concurrent.atomic.AtomicBoolean;

public class ClusterLock {

    private final AtomicBoolean isLock = new AtomicBoolean(false);
    private final String resourceName;

    public ClusterLock(String resourceName){
        this.resourceName = resourceName;
    }

    public void lock() {
        for(;;){
            if(isLock.compareAndSet(false, true))
                return;

            Thread.onSpinWait();
        }
    }

    public void unlock() {
        isLock.set(false);
    }

    public boolean isLocked() {
        return isLock.get();
    }

    public String getResourceName() {
        return resourceName;
    }
}
