package hello1.koddata.concurrent;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
// count across cluster
public class IdCounter {
    private final AtomicLong counter = new AtomicLong(0);

    public long next(){
        return counter.getAndIncrement();
    }

}
