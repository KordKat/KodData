package hello1.koddata.concurrent.cluster;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ClusterMessageBus {

    private static final BlockingQueue<ClusterMessage> queue =
            new LinkedBlockingQueue<>();

    public static void push(ClusterMessage msg){
        queue.offer(msg);
    }

    public static ClusterMessage take() throws InterruptedException {
        return queue.take();
    }
}
