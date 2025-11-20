package hello1.koddata.net;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import hello1.koddata.Main;
import hello1.koddata.engine.DataName;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.io.ChannelState;
import hello1.koddata.sessions.Session;
import hello1.koddata.utils.Serializable;
import hello1.koddata.utils.ref.ClusterReference;

public class DataTransferServer extends Server implements Runnable {

    private static final byte OP_REQUEST_DATA = 0x00;
    private static final byte OP_RESPONSE_DATA = 0x01;

    private static final byte OP_SESSION_DATA_SEND = 0x10;
    private static final byte OP_SESSION_DATA_REQUEST = 0x11;

    private static final byte OP_RESOURCE_REQUEST = 0x20;
    private static final byte OP_RESOURCE_SEND = 0x21;

    private Set<InetSocketAddress> peers;
    private ConcurrentMap<InetSocketAddress, SocketChannel> sockets;
    private ConcurrentMap<SocketChannel, InetSocketAddress> channelToAddress;

    public volatile boolean running;
    private Thread serverThread;
    private Selector selector;
    private ServerSocketChannel ssc;

    public DataTransferServer(InetSocketAddress inetSocketAddress, SocketFactory socketFactory) {
        super(inetSocketAddress, socketFactory);
        peers = new HashSet<>();
        sockets = new ConcurrentHashMap<>();
        channelToAddress = new ConcurrentHashMap<>();
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
        readerLoop();
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

            NetUtils.sendMessage(ch, NetUtils.OPCODE_DATATRANSFER);

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


    public void transfer(InetSocketAddress node, Serializable serializable, long sessionId, long blockId) throws KException {
        SocketChannel ch = sockets.get(node);
        if (ch == null || !ch.isConnected()) {
            throw new KException(ExceptionCode.KDN0011, "Peer node is not connected: " + node);
        }

        byte[] data = serializable.serialize();
        ByteBuffer buf = ByteBuffer.allocate(data.length + 4);
        Integer wireId = Serializable.searchWireId(serializable.getClass());

        if (wireId == null) {
            throw new KException(ExceptionCode.KD00000, "Wire ID not registered for class: " + serializable.getClass().getName());
        }

        buf.putLong(sessionId);
        buf.putLong(blockId);
        buf.putInt(wireId);
        buf.put(data);

        buf.flip();

        try {
            while (buf.hasRemaining()) {
                ch.write(buf);
            }
        } catch (IOException e) {

        }
    }

    public Object read(ByteBuffer buf) throws KException {
        if (buf.remaining() < 4) {
            throw new KException(ExceptionCode.KD00000, "Buffer too short to read wire ID.");
        }

        final int wireId = buf.getInt();

        Class<? extends Serializable> wireClass = Serializable.searchWireClass(wireId);
        if (wireClass == null) {
            throw new KException(ExceptionCode.KDS00014, "Unknown wire ID received: " + wireId);
        }

        byte[] data = new byte[buf.remaining()];
        buf.get(data);

        try {
            Serializable instance = wireClass.getDeclaredConstructor().newInstance();
            instance.deserialize(data);
            return instance;

        } catch (Exception e) {
            throw new KException(ExceptionCode.KD00000, "Error during deserialization or instantiation: " + e.getMessage());
        }
    }

    public void readerLoop() {
        final int READ_BUFFER_SIZE = 64 * 1024; // 64KB chunks
        Map<SocketChannel, ChannelState> states = new ConcurrentHashMap<>();

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
                        SocketChannel ch = (SocketChannel) key.channel();
                        ChannelState state = states.computeIfAbsent(ch, c -> new ChannelState(READ_BUFFER_SIZE));

                        try {
                            if (!state.headerComplete) {
                                int n = ch.read(state.headerBuffer);
                                if (n == -1) {
                                    closeChannel(ch, key, states);
                                    continue;
                                }
                                if (state.headerBuffer.hasRemaining()) {
                                    continue; // header not yet fully received
                                }

                                // Parse header
                                state.headerBuffer.flip();
                                state.sessionId = state.headerBuffer.getLong();
                                state.blockId = state.headerBuffer.getLong();
                                state.payloadLength = state.headerBuffer.getLong();
                                state.headerBuffer.clear();
                                state.headerComplete = true;

                                state.tempFile = Files.createTempFile("batch_", ".bin");
                                state.output = Files.newOutputStream(state.tempFile);
                                state.bytesReceived = 0;

                                state.payloadBuffer.clear();
                            }

                            int n = ch.read(state.payloadBuffer);
                            if (n == -1) {
                                closeChannel(ch, key, states);
                                continue;
                            }

                            state.payloadBuffer.flip();
                            state.output.write(state.payloadBuffer.array(), 0, state.payloadBuffer.limit());
                            state.bytesReceived += state.payloadBuffer.limit();
                            state.payloadBuffer.clear();

                            if (state.bytesReceived >= state.payloadLength) {
                                state.output.close();

                                Object o = deserializeLargeObject(state.tempFile);

                                Session session = Main.bootstrap.getSessionManager().getSession(state.sessionId);
                                String varName = session.getSessionData().getPreparedBlock().get(state.blockId);

                                DataName name = varName.contains("$")
                                        ? new DataName(varName.split("\\$")[0], varName.split("\\$")[1])
                                        : new DataName(varName, null);

                                session.getSessionData().assignVariable(name, o);

                                state.reset();
                            }
                        } catch (Exception ex) {

                        }
                    }
                }
            } catch (IOException e) {

            }
        }
    }

    public <T> T requestData(ClusterReference<T> ref){
        return null;
    }

    private Object deserializeLargeObject(Path file) throws Exception {
        try (ObjectInputStream ois = new ObjectInputStream(Files.newInputStream(file))) {
            return ois.readObject();
        }
    }

    private void closeChannel(SocketChannel ch, SelectionKey key, Map<SocketChannel, ChannelState> states) {
        try {
            key.cancel();
            states.remove(ch);
            ch.close();
        } catch (IOException ignored) {}
    }



}