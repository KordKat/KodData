package hello1.koddata.net;

import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.*;

public class GossipServer extends Server {




    private Set<InetSocketAddress> peers;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService senderExec = Executors.newFixedThreadPool(4);
    private final ExecutorService readerExec = Executors.newSingleThreadExecutor();

    private final ConcurrentMap<InetSocketAddress, NodeStatus> statusMap = new ConcurrentHashMap<>();

    public GossipServer(InetSocketAddress inetSocketAddress, SocketFactory socketFactory, Properties properties) throws KException {
        super(inetSocketAddress, socketFactory);
        String peerString = properties.getProperty("peers", "");
        String[] peerList = peerString.split(",");
        peers = new HashSet<>();
        for (String peer : peerList) {
            peers.add(convertToInetSocketAddress(peer.trim().toLowerCase()));
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

    @Override
    public void start() {
        running = true;
        for (InetSocketAddress inet : peers) {
            NodeStatus ns = new NodeStatus();
            try {
                SocketChannel channel = factory.createNode(inet.getHostName(), inet.getPort());
                channel.configureBlocking(false);
                ns.setAvailable(true);
                ns.setChannel(channel);
                statusMap.put(inet, ns);

                ByteBuffer greeting = ByteBuffer.allocate(1);
                greeting.put(NetUtils.OPCODE_STARTUP);
                greeting.flip();
                channel.write(greeting);
            } catch (IOException ignored) {
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
        for (InetSocketAddress peer : peers) {
            senderExec.submit(() -> {
                sendHeartbeat(peer);
                sendStatus(peer);
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
                ns.getChannel().write(bye);
                ns.getChannel().close();
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
        ns.setChannel(ch);
    }

    private void readFromPeer(SelectionKey key, ByteBuffer buffer) {
        SocketChannel ch = (SocketChannel) key.channel();
        buffer.clear();
        try {
            int n = ch.read(buffer);
            if (n <= 0) {
                if (n == -1) {
                    key.cancel();
                    ch.close();
                }
                return;
            }
            buffer.flip();
            byte opcode = buffer.get();
            if (opcode == NetUtils.OPCODE_HEARTBEAT) return;
            if (opcode == NetUtils.OPCODE_STATUS) {
                byte[] body = new byte[buffer.remaining()];
                buffer.get(body);
                processStatusMessage(ch, body);
            }
        } catch (IOException ignored) {
            try {
                ch.close();
            } catch (IOException ignored2) {
            }
            key.cancel();
        }
    }

    private void processStatusMessage(SocketChannel ch, byte[] payload) {
        InetSocketAddress addr;
        try {
            addr = (InetSocketAddress) ch.getRemoteAddress();
        } catch (IOException e) {
            return;
        }
        NodeStatus status = decodeReceivedStatus(payload);
        if (status != null) {
            statusMap.put(addr, status);
        }
    }

    public ConcurrentMap<InetSocketAddress, NodeStatus> getStatusMap() {
        return statusMap;
    }

    private byte[] encodeStatusForSending() {
        return encodeNodeStatus();
    }

    private byte[] encodeNodeStatus() {
        return new byte[0];
    }

    private NodeStatus decodeReceivedStatus(byte[] data) {
        return null;
    }
}
