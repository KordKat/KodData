package hello1.koddata.net;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import hello1.koddata.Main;
import hello1.koddata.engine.DataName;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.io.ChannelState;
import hello1.koddata.io.ServerStateChannelState;
import hello1.koddata.io.SessionBlockChannelState;
import hello1.koddata.io.UserFileChannelState;
import hello1.koddata.sessions.Session;
import hello1.koddata.utils.Serializable;
import hello1.koddata.utils.ref.ClusterReference;
import hello1.koddata.utils.ref.ReplicatedResourceClusterReference;

public class DataTransferServer extends Server implements Runnable {

    private static final byte MODE_SESSION_BLOCK = 0x30;
    private static final byte MODE_USER_FILE = 0x31;
    private static final byte MODE_SERVER_STATE = 0x32;
    public static final byte MODE_SERVER_STATE_FEEDBACK = 0x33;

    private Set<InetSocketAddress> peers;
    private ConcurrentMap<InetSocketAddress, SocketChannel> sockets;
    private ConcurrentMap<SocketChannel, InetSocketAddress> channelToAddress;
    private ConcurrentMap<SocketChannel, ChannelState> readStates;
    private ConcurrentMap<SocketChannel, Queue<ByteBuffer>> writeQueues;

    private Thread serverThread;
    private Selector selector;
    private ServerSocketChannel ssc;

    public DataTransferServer(InetSocketAddress inetSocketAddress, SocketFactory socketFactory) {
        super(inetSocketAddress, socketFactory);
        peers = new HashSet<>();
        sockets = new ConcurrentHashMap<>();
        channelToAddress = new ConcurrentHashMap<>();
        readStates = new ConcurrentHashMap<>();
        writeQueues = new ConcurrentHashMap<>();
    }

    @Override
    public void start() throws IOException {
        selector = Selector.open();
        ssc = factory.createServer(inetSocketAddress.getPort());
        ssc.register(selector, SelectionKey.OP_ACCEPT);

        running = true;
        serverThread = new Thread(this, "DataTransferServerThread");
        serverThread.start();
    }

    @Override
    public void stop() throws IOException {
        running = false;
        if (selector != null) {
            selector.wakeup();
        }
        if (serverThread != null) {
            serverThread.interrupt();
        }
        if (ssc != null) {
            ssc.close();
        }
    }

    @Override
    public void run() {
        eventLoop();
    }

    public void addPeer(InetSocketAddress isa) throws IOException {
        peers.add(isa);
        if (running) {
            SocketChannel ch = factory.createNode(isa.getHostName(), isa.getPort());
            ch.register(selector, SelectionKey.OP_CONNECT, isa);
            selector.wakeup();
        }
    }

    private void completeConnection(SelectionKey key, SocketChannel ch, InetSocketAddress isa) throws IOException {
        try {
            if (ch.isConnectionPending()) {
                ch.finishConnect();
            }

            NetUtils.sendMessage(ch, NetUtils.OPCODE_DATATRANSFER_HANDSHAKE);

            sockets.put(isa, ch);
            channelToAddress.put(ch, isa);

            ch.register(selector, SelectionKey.OP_READ);

        } catch (IOException e) {
            ch.close();
            key.cancel();
            peers.remove(isa);
        }
    }

    private void acceptConnection(SelectionKey key) throws IOException {
        ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
        SocketChannel ch = serverChannel.accept();
        if (ch != null) {
            ch.configureBlocking(false);
            sockets.put((InetSocketAddress) ch.getRemoteAddress(), ch);
            channelToAddress.put(ch, (InetSocketAddress) ch.getRemoteAddress());
            ch.register(selector, SelectionKey.OP_READ);
        }
    }

    private void queueWrite(SocketChannel ch, ByteBuffer buffer) {
        writeQueues.computeIfAbsent(ch, k -> new ConcurrentLinkedQueue<>()).add(buffer);
        SelectionKey key = ch.keyFor(selector);
        if (key != null && key.isValid()) {
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            selector.wakeup();
        }
    }

    //sending variable across of node
    public void transfer(InetSocketAddress node, Serializable serializable, long sessionId, String name, String index) throws KException {
        SocketChannel ch = sockets.get(node);
        if (ch == null || !ch.isConnected()) {
            throw new KException(ExceptionCode.KDN0011, "Peer node is not connected: " + node);
        }
        byte[] data = serializable.serialize();
        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        byte[] indexBytes = index == null ? new byte[0] : index.getBytes(StandardCharsets.UTF_8);

        long capacity = 8 + 1 + 8 + 4 + nameBytes.length + 4 + indexBytes.length + 4 + data.length;

        ByteBuffer buf = ByteBuffer.allocate((int) capacity);
        Integer wireId = Serializable.searchWireId(serializable.getClass());
        byte mode = MODE_SESSION_BLOCK;
        if (wireId == null) {
            throw new KException(ExceptionCode.KD00000, "Wire ID not registered for class: " + serializable.getClass().getName());
        }
        buf.putLong(capacity);
        buf.put(mode);
        buf.putLong(sessionId);
        buf.putInt(nameBytes.length);
        buf.put(nameBytes);
        buf.putInt(indexBytes.length);
        buf.put(indexBytes);
        buf.putInt(wireId);
        buf.put(data);
        buf.flip();
        queueWrite(ch, buf);
    }

    //sending file across of node
    public void transfer(InetSocketAddress node, Serializable serializable, long userId, String fileName) throws KException {
        SocketChannel ch = sockets.get(node);
        if (ch == null || !ch.isConnected()) {
            throw new KException(ExceptionCode.KDN0011, "Peer node is not connected: " + node);
        }

        byte[] data = serializable.serialize();
        byte[] fName = fileName.getBytes(StandardCharsets.UTF_8);

        long capacity = 8 + 1 + 8 + 4 + fName.length + data.length;
        byte mode = MODE_USER_FILE;
        ByteBuffer buf = ByteBuffer.allocate((int) capacity);

        buf.putLong(capacity);
        buf.put(mode);
        buf.putLong(userId);
        buf.putInt(fName.length);
        buf.put(fName);
        buf.put(data);
        buf.flip();
        queueWrite(ch, buf);
    }

    //sending node replicated resource for consistency
    public void transfer(InetSocketAddress node, ConcurrentMap<String, ReplicatedResourceClusterReference<?>> resources) throws KException, IOException {
        SocketChannel ch = sockets.get(node);
        if (ch == null || !ch.isConnected()) {
            throw new KException(ExceptionCode.KDN0011, "Peer node is not connected: " + node);
        }

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(ByteBuffer.allocate(4).putInt(resources.size()).array());

        for (Map.Entry<String, ReplicatedResourceClusterReference<?>> r : resources.entrySet()) {
            String name = r.getValue().getResourceName();
            byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
            byte[] criteriaBytes = r.getValue().get().getConsistencyCriteria().serialize();

            bos.write(ByteBuffer.allocate(4).putInt(nameBytes.length).array());
            bos.write(nameBytes);

            bos.write(ByteBuffer.allocate(4).putInt(criteriaBytes.length).array());
            bos.write(criteriaBytes);
        }

        byte[] dataset = bos.toByteArray();
        byte mode = MODE_SERVER_STATE;
        long capacity = 8 + 1 + dataset.length;

        ByteBuffer buf = ByteBuffer.allocate(8 + 1 + dataset.length);
        buf.putLong(capacity);
        buf.put(mode);
        buf.put(dataset);
        buf.flip();

        queueWrite(ch, buf);
    }

    //feedback when data is inconsistent
    public void sendFeedback(SocketChannel ch, byte[] data) {
        long capacity = 8 + 1 + data.length;
        ByteBuffer buf = ByteBuffer.allocate((int)capacity);
        buf.putLong(capacity);
        buf.put(MODE_SERVER_STATE_FEEDBACK);
        buf.put(data);
        buf.flip();
        queueWrite(ch, buf);
    }

    public void eventLoop() {
        while (running) {
            try {
                selector.select();

                Iterator<SelectionKey> keyIterator = selector.selectedKeys().iterator();
                while (keyIterator.hasNext()) {
                    SelectionKey key = keyIterator.next();
                    keyIterator.remove();

                    if (!key.isValid())
                        continue;

                    if (key.isAcceptable()) {
                        acceptConnection(key);
                        continue;
                    }

                    if (key.isConnectable()) {
                        completeConnection(key, (SocketChannel) key.channel(), (InetSocketAddress) key.attachment());
                        continue;
                    }

                    if (key.isReadable()) {
                        handleRead(key);
                    }

                    if (key.isWritable()) {
                        handleWrite(key);
                    }
                }
            } catch (IOException e) {
            }
        }
    }

    private void handleWrite(SelectionKey key) throws IOException {
        SocketChannel ch = (SocketChannel) key.channel();
        Queue<ByteBuffer> queue = writeQueues.get(ch);
        if (queue == null || queue.isEmpty()) {
            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
            return;
        }
        ByteBuffer buffer = queue.peek();
        if (buffer != null) {
            ch.write(buffer);
            if (!buffer.hasRemaining()) {
                queue.poll();
            }
        }
        if (queue.isEmpty()) {
            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
        }
    }

    private void handleRead(SelectionKey key) {
        SocketChannel ch = (SocketChannel) key.channel();
        ChannelState state = readStates.get(ch);

        try {
            if (state == null) {
                ByteBuffer headerBuf = ByteBuffer.allocate(9);
                int read = ch.read(headerBuf);
                if (read == -1) {
                    closeChannel(ch, key);
                    return;
                }
                if (read < 9) return;
                headerBuf.flip();
                long capacity = headerBuf.getLong();
                byte mode = headerBuf.get();

                switch (mode) {
                    case MODE_SESSION_BLOCK:
                        state = new SessionBlockChannelState((int) capacity, Main.bootstrap.getSessionManager());
                        break;
                    case MODE_USER_FILE:
                        state = new UserFileChannelState((int) capacity, Main.bootstrap.getUserManager());
                        break;
                    case MODE_SERVER_STATE:
                        state = new ServerStateChannelState((int) capacity, this, ch, false);
                        break;
                    case MODE_SERVER_STATE_FEEDBACK:
                        state = new ServerStateChannelState((int) capacity, this, ch, true);
                        break;
                    default:
                        closeChannel(ch, key);
                        return;
                }
                state.payloadLength = capacity - 9;
                readStates.put(ch, state);
            }

            int bytesRead = ch.read(state.payloadBuffer);
            if (bytesRead == -1) {
                closeChannel(ch, key);
                return;
            }
            state.bytesReceived += bytesRead;

            if (state.bytesReceived >= state.payloadLength) {
                state.payloadBuffer.flip();
                state.perform();
                readStates.remove(ch);
            }

        } catch (Exception e) {
            closeChannel(ch, key);
        }
    }

    public <T> T requestData(ClusterReference<T> ref){
        return null;
    }

    private void closeChannel(SocketChannel ch, SelectionKey key) {
        try {
            key.cancel();
            sockets.values().remove(ch);
            readStates.remove(ch);
            writeQueues.remove(ch);
            ch.close();
        } catch (IOException ignored) {}
    }

}