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
import hello1.koddata.utils.ref.ReplicatedResourceClusterReference;

public class DataTransferServer extends Server implements Runnable {

    private static final byte OP_REQUEST_DATA = 0x00;
    private static final byte OP_RESPONSE_DATA = 0x01;

    private static final byte OP_SESSION_DATA_SEND = 0x10;
    private static final byte OP_SESSION_DATA_REQUEST = 0x11;

    private static final byte OP_RESOURCE_REQUEST = 0x20;
    private static final byte OP_RESOURCE_SEND = 0x21;

    private static final byte MODE_SESSION_BLOCK = 0x30;
    private static final byte MODE_USER_FILE = 0x31;
    private static final byte MODE_SERVER_STATE = 0x32;

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

    //to session block
    public void transfer(InetSocketAddress node, Serializable serializable, long sessionId, String name, String index) throws KException {
        SocketChannel ch = sockets.get(node);
        if (ch == null || !ch.isConnected()) {
            throw new KException(ExceptionCode.KDN0011, "Peer node is not connected: " + node);
        }
        byte[] data = serializable.serialize();
        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        byte[] indexBytes = index == null ? new byte[]{0} : index.getBytes(StandardCharsets.UTF_8);
        long capacity = data.length + 21 + nameBytes.length + indexBytes.length;
        ByteBuffer buf = ByteBuffer.allocate((int) capacity);
        Integer wireId = Serializable.searchWireId(serializable.getClass());
        byte mode = MODE_SESSION_BLOCK;
        if (wireId == null) {
            throw new KException(ExceptionCode.KD00000, "Wire ID not registered for class: " + serializable.getClass().getName());
        }
        buf.putLong(capacity);
        buf.put(mode);
        buf.putLong(sessionId);
        buf.put(nameBytes);
        buf.put(indexBytes);
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

    //to user file
    public void transfer(InetSocketAddress node, Serializable serializable, long userId, String fileName) throws KException {
        SocketChannel ch = sockets.get(node);
        if (ch == null || !ch.isConnected()) {
            throw new KException(ExceptionCode.KDN0011, "Peer node is not connected: " + node);
        }

        byte[] data = serializable.serialize();
        Integer wireId = Serializable.searchWireId(serializable.getClass());
        byte[] fName = fileName.getBytes(StandardCharsets.UTF_8);
        long capacity = data.length + 17 + fName.length;
        byte mode = MODE_USER_FILE;
        ByteBuffer buf = ByteBuffer.allocate((int) capacity);

        buf.putLong(capacity); //8
        buf.put(mode); //1
        buf.putLong(userId); //8
        buf.put(fName);
        buf.put(data);
        buf.flip();

        try {
            while (buf.hasRemaining()) {
                ch.write(buf);
            }
        } catch (IOException e) {

        }
    }

    public void transfer(InetSocketAddress node, ConcurrentMap<String, ReplicatedResourceClusterReference<?>> resources) throws KException, IOException {
        SocketChannel ch = sockets.get(node);
        if (ch == null || !ch.isConnected()) {
            throw new KException(ExceptionCode.KDN0011, "Peer node is not connected: " + node);
        }

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
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

        ByteBuffer buf = ByteBuffer.allocate((int) capacity);
        buf.putLong(capacity);
        buf.put(mode);
        buf.put(dataset);
        buf.flip();

        try {
            while (buf.hasRemaining()) {
                ch.write(buf);
            }
        } catch (IOException e) {

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