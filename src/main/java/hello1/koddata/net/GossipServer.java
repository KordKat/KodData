package hello1.koddata.net;

import hello1.koddata.engine.Bootstrap;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.utils.ref.ReplicatedResourceClusterReference;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.*;

public class GossipServer extends Server {

    private Set<InetSocketAddress> peers;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService senderExec = Executors.newFixedThreadPool(4);
    private final ExecutorService readerExec = Executors.newSingleThreadExecutor();

    private static final float GOSSIP_FRACTION = 0.3f;
    // 10 Seconds timeout constant
    private static final long FAILURE_TIMEOUT_MS = 10000;

    private DataTransferServer dataTransferServer;

    private final ConcurrentMap<InetSocketAddress, NodeStatus> statusMap = new ConcurrentHashMap<>();

    public GossipServer(InetSocketAddress inetSocketAddress, SocketFactory socketFactory) throws KException {
        super(inetSocketAddress, socketFactory);
        String peerString = Bootstrap.getConfig().getProperty("peers", "");
        String[] peerList = peerString.split(",");
        peers = new HashSet<>();
        for (String peer : peerList) {
            if (!peer.trim().isEmpty()) {
                peers.add(convertToInetSocketAddress(peer.trim().toLowerCase()));
            }
        }
    }

    private InetSocketAddress convertToInetSocketAddress(String hostname) throws KException {
        int colonIndex = hostname.lastIndexOf(":");
        if (colonIndex == -1) {
            throw new KException(ExceptionCode.KDN0011, "Invalid hostname " + hostname);
        }
        String host = hostname.substring(0, colonIndex);
        String portStr = hostname.substring(colonIndex + 1);
        int port = Integer.parseInt(portStr);
        return new InetSocketAddress(host, port);
    }

    public void setDataTransferServer(DataTransferServer dataTransferServer) {
        this.dataTransferServer = dataTransferServer;
    }

    @Override
    public void start() {
        running = true;
        long now = System.currentTimeMillis();

        for (InetSocketAddress inet : peers) {
            NodeStatus ns = new NodeStatus();
            try {
                SocketChannel channel = factory.createNode(inet.getHostName(), inet.getPort());
                channel.configureBlocking(false);
                ns.setAvailable(true);
                ns.setChannel(channel);
                ns.setLastHeartbeatTime(now);
                statusMap.put(inet, ns);

                ByteBuffer greeting = ByteBuffer.allocate(1);
                greeting.put(NetUtils.OPCODE_STARTUP);
                greeting.flip();
                channel.write(greeting);
            } catch (IOException ignored) {
                ns.setAvailable(false); // Mark unavailable if connection fails initially
                statusMap.put(inet, ns);
            }
        }
        try {
            serverChannel = factory.createServer(inetSocketAddress.getPort());
            selector = Selector.open();
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        scheduler.scheduleAtFixedRate(this::run, 0, 2, TimeUnit.SECONDS);
        readerExec.submit(this::readerLoop);
    }

    public void run() {
        if (!running) return;

        long currentTime = System.currentTimeMillis();

        for (Map.Entry<InetSocketAddress, NodeStatus> entry : statusMap.entrySet()) {
            NodeStatus ns = entry.getValue();
            if (ns.isAvailable() && (currentTime - ns.getLastHeartbeatTime() > FAILURE_TIMEOUT_MS)) {
                ns.setAvailable(false);
            }
        }

        List<InetSocketAddress> allPeers = new ArrayList<>(peers);
        Collections.shuffle(allPeers);

        int count = Math.max(1, (int) (peers.size() * GOSSIP_FRACTION));
        List<InetSocketAddress> selectedPeers = allPeers.subList(0, Math.min(count, allPeers.size()));

        for (InetSocketAddress peer : selectedPeers) {
            senderExec.submit(() -> {
                sendHeartbeat(peer);
                sendStatus(peer);
                consistentCheck(peer);
            });
        }
    }

    @Override
    public void stopGracefully() {
        running = false;
        scheduler.shutdown();
        senderExec.shutdown();
        readerExec.shutdown();
        for (NodeStatus ns : statusMap.values()) {
            if (!ns.isAvailable()) continue;
            ByteBuffer bye = ByteBuffer.allocate(1);
            bye.put(NetUtils.OPCODE_SHUTDOWN);
            bye.flip();
            try {
                if (ns.getChannel() != null && ns.getChannel().isOpen()) {
                    ns.getChannel().write(bye);
                    ns.getChannel().close();
                }
            } catch (IOException ignored) {
            }
        }
        try {
            if (selector != null) selector.close();
            if (serverChannel != null) serverChannel.close();
        } catch (IOException ignored) {
        }
        try {
            scheduler.awaitTermination(2, TimeUnit.SECONDS);
            senderExec.awaitTermination(2, TimeUnit.SECONDS);
            readerExec.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
        }
    }

    @Override
    public void stop() {
        stopGracefully();
    }

    public void consistentCheck(InetSocketAddress peer) {
        ConcurrentMap<String, ReplicatedResourceClusterReference<?>> resources = ReplicatedResourceClusterReference.resources;
        if(dataTransferServer != null && dataTransferServer.running){
            try {
                dataTransferServer.transfer(peer, resources);
            } catch (IOException | KException ignored) {
            }
        }
    }

    private void sendHeartbeat(InetSocketAddress peer) {
        NodeStatus ns = statusMap.get(peer);
        if (ns == null) return;
        SocketChannel ch = ns.getChannel();
        try {
            if (ch == null || !ch.isConnected() || !ch.isOpen()) {
                ch = SocketChannel.open(peer);
                ch.configureBlocking(false);
                ns.setChannel(ch);
            }
            ByteBuffer b = ByteBuffer.allocate(1);
            b.put(NetUtils.OPCODE_HEARTBEAT);
            b.flip();
            while (b.hasRemaining()) {
                if (ch.write(b) == 0) Thread.yield();
            }
        } catch (Exception ignored) {
        }
    }

    private void sendStatus(InetSocketAddress peer) {
        NodeStatus ns = statusMap.get(peer);
        if (ns == null) return;
        SocketChannel ch = ns.getChannel();
        try {
            if (ch == null || !ch.isConnected() || !ch.isOpen()) {
                ch = factory.createNode(peer.getHostName(), peer.getPort());
                ch.configureBlocking(false);
                ns.setChannel(ch);
            }
            byte[] payload = encodeStatusForSending();
            ByteBuffer b = ByteBuffer.allocate(1 + payload.length);
            b.put(NetUtils.OPCODE_STATUS);
            b.put(payload);
            b.flip();
            while (b.hasRemaining()) {
                if (ch.write(b) == 0) Thread.yield();
            }
        } catch (Exception ignored) {
        }
    }

    private void readerLoop() {
        ByteBuffer buffer = ByteBuffer.allocate(2048);
        while (running) {
            try {
                selector.select(500);
                Iterator<SelectionKey> it = selector.selectedKeys().iterator();
                while (it.hasNext()) {
                    SelectionKey key = it.next();
                    it.remove();
                    if (key.isAcceptable()) acceptConnection();
                    else if (key.isReadable()) readFromPeer(key, buffer);
                }
            } catch (Exception ignored) {
            }
        }
        try {
            selector.close();
            serverChannel.close();
        } catch (IOException ignored) {
        }
    }

    private void acceptConnection() throws IOException {
        SocketChannel ch = serverChannel.accept();
        if (ch == null) return;
        ch.configureBlocking(false);
        ch.register(selector, SelectionKey.OP_READ);
        InetSocketAddress addr = (InetSocketAddress) ch.getRemoteAddress();
        NodeStatus ns = statusMap.get(addr);
        if (ns == null) {
            ns = new NodeStatus();
            statusMap.put(addr, ns);
        }
        ns.setAvailable(true);
        ns.setLastHeartbeatTime(System.currentTimeMillis());
        ns.setChannel(ch);
        ch.keyFor(selector).attach(ns);
    }

    private void readFromPeer(SelectionKey key, ByteBuffer buffer) {
        SocketChannel ch = (SocketChannel) key.channel();
        NodeStatus ns = (NodeStatus) key.attachment();

        buffer.clear();
        try {
            int n = ch.read(buffer);
            if (n <= 0) {
                if (n == -1) {
                    key.cancel();
                    ch.close();
                    if(ns != null) ns.setAvailable(false);
                }
                return;
            }

            if (ns != null) {
                ns.setLastHeartbeatTime(System.currentTimeMillis());
                ns.setAvailable(true);
            }

            buffer.flip();
            byte opcode = buffer.get();

            if (opcode == NetUtils.OPCODE_HEARTBEAT) return;

            if (opcode == NetUtils.OPCODE_STATUS) {
                byte[] body = new byte[buffer.remaining()];
                buffer.get(body);
                processStatusMessage(ch, body);
            } else if(opcode == NetUtils.OPCODE_INFO_DATA_PORT) {
                byte[] body = new byte[buffer.remaining()];
                buffer.get(body);
                ByteBuffer portBuf = ByteBuffer.wrap(body);
                int i = portBuf.getInt();
                InetSocketAddress isa = (InetSocketAddress) ch.getRemoteAddress();
                dataTransferServer.addPeer(new InetSocketAddress(isa.getHostName(), i));
            } else if(opcode == NetUtils.OPCODE_STARTUP) {
                InetSocketAddress isa = (InetSocketAddress) ch.getRemoteAddress();
                if(!statusMap.containsKey(isa)) return;
                ByteBuffer buf = ByteBuffer.allocate(5);
                buf.put(NetUtils.OPCODE_INFO_DATA_PORT);
                buf.putInt(Integer.parseInt((String) Bootstrap.getConfig().get("server.data.port")));
                buf.flip();
                byte[] d = new byte[buf.capacity()];
                buf.get(d);
                NetUtils.sendMessage(ch, d);
            }
        } catch (IOException ignored) {
            try {
                ch.close();
            } catch (IOException ignored2) {
            }
            key.cancel();
            if(ns != null) ns.setAvailable(false);
        }
    }

    private void processStatusMessage(SocketChannel ch, byte[] payload) {
        InetSocketAddress addr;
        try {
            addr = (InetSocketAddress) ch.getRemoteAddress();
        } catch (IOException e) {
            return;
        }
        NodeStatus receivedStatus = decodeReceivedStatus(payload);

        if (receivedStatus != null) {
            NodeStatus current = statusMap.get(addr);
            if (current != null) {
                current.setLastHeartbeatTime(System.currentTimeMillis());
                current.setAvailable(receivedStatus.isAvailable());
            } else {
                receivedStatus.setChannel(ch);
                receivedStatus.setLastHeartbeatTime(System.currentTimeMillis());
                statusMap.put(addr, receivedStatus);
            }
        }
    }

    public ConcurrentMap<InetSocketAddress, NodeStatus> getStatusMap() {
        return statusMap;
    }

    private byte[] encodeStatusForSending() {
        return encodeNodeStatus();
    }

    private byte[] encodeNodeStatus() {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + 1);
        buffer.putLong(System.currentTimeMillis());
        buffer.put((byte) 1);
        return buffer.array();
    }

    private NodeStatus decodeReceivedStatus(byte[] data) {
        if (data.length < 9) return null; // Basic validation
        ByteBuffer buffer = ByteBuffer.wrap(data);

        long remoteTimestamp = buffer.getLong();
        boolean isAvailable = buffer.get() == 1;

        NodeStatus status = new NodeStatus();
        status.setAvailable(isAvailable);
        return status;
    }

    public Set<InetSocketAddress> getPeers() {
        return peers;
    }

    public InetSocketAddress getDataTransferServerInetSocketAddress() {
        return dataTransferServer.getInetSocketAddress();
    }

    public Set<NodeStatus> selectNode(long dfSize) {

        float requiredSpace = (float) dfSize;

        List<NodeStatus> candidates = statusMap.values()
                .stream()
                .filter(NodeStatus::isAvailable)
                .filter(n -> (n.getMemoryLoad()) < 1)
                .sorted((a, b) -> {
                    double freeA = a.getFullMemory() * (1 - a.getMemoryLoad());
                    double freeB = b.getFullMemory() * (1 - b.getMemoryLoad());
                    return Double.compare(freeB, freeA);
                })
                .toList();

        Set<NodeStatus> selected = new LinkedHashSet<>();
        float accumulated = 0;

        for (NodeStatus node : candidates) {
            selected.add(node);
            accumulated += (node.getFullDisk() - node.getDiskUsage());

            if (accumulated >= requiredSpace) {
                return selected;
            }
        }
        return Collections.emptySet();
    }

}