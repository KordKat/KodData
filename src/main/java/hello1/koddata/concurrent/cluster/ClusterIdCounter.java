package hello1.koddata.concurrent.cluster;

import hello1.koddata.net.NetUtils;
import hello1.koddata.net.NodeStatus;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class ClusterIdCounter {

    private static final ConcurrentMap<String, ClusterIdCounter> clusterIdCounters = new ConcurrentHashMap<>();
    private static final long RESPONSE_TIMEOUT_MS = 500;

    private final AtomicLong idCounter = new AtomicLong(0);
    private final String name;
    private final Set<NodeStatus> peers;

    private final ConcurrentMap<String, Long> pendingResponses = new ConcurrentHashMap<>();
    private final CountDownLatch latch = new CountDownLatch(1);

    public ClusterIdCounter(Class<?> clazz, Set<NodeStatus> peers){
        this.name = clazz.getName();
        this.peers = peers;
    }

    public static ClusterIdCounter getCounter(Class<?> clazz, Set<NodeStatus> peers){
        return clusterIdCounters.computeIfAbsent(clazz.getName(),
                k -> new ClusterIdCounter(clazz, peers));
    }

    public void setCount(long newCount){
        idCounter.set(newCount);
    }

    public long count(){
        if(peers == null || peers.isEmpty())
            return idCounter.incrementAndGet();

        pendingResponses.clear();

        for(NodeStatus ns : peers){
            if(!ns.isAvailable()) continue;

            try{
                NetUtils.sendMessage(ns.getChannel(),
                        NetUtils.OPCODE_COUNT,
                        name);
            } catch (Exception e){
                ns.setAvailable(false);
            }
        }

        try {
            Thread.sleep(RESPONSE_TIMEOUT_MS);
        } catch (InterruptedException ignored) {}

        long max = idCounter.get();

        for(long v : pendingResponses.values()){
            if(v > max) max = v;
        }

        long next = max + 1;
        idCounter.set(next);

        return next;
    }

    public void handleCountRequest(SocketChannel channel){
        try {
            NetUtils.sendMessage(channel,
                    NetUtils.OPCODE_COUNT_RESPONSE,
                    name,
                    String.valueOf(idCounter.get()));
        } catch (IOException ignored) {}
    }

    public void handleCountResponse(String counterName, long value){
        if(counterName.equals(name)) {
            pendingResponses.put(UUID.randomUUID().toString(), value);
        }
    }

    public String getName() {
        return name;
    }
}
