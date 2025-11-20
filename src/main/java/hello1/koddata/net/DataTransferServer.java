package hello1.koddata.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.Set;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import hello1.koddata.Main;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.sessions.Session;
import hello1.koddata.utils.Serializable;

public class DataTransferServer extends Server implements Runnable {

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
        ssc.bind(inetSocketAddress);
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
            return instance.deserialize(data);

        } catch (Exception e) {
            throw new KException(ExceptionCode.KD00000, "Error during deserialization or instantiation: " + e.getMessage());
        }
    }

    public void readerLoop() {
        final int MAX_READ_SIZE = 4096;

        while (running) {
            try {
                selector.select();

                Iterator<SelectionKey> keyIterator = selector.selectedKeys().iterator();
                while (keyIterator.hasNext()) {
                    SelectionKey key = keyIterator.next();
                    keyIterator.remove();

                    if (!key.isValid()) {
                        continue;
                    }

                    if (key.isAcceptable()) {
                        acceptConnection(key);
                    } else if (key.isConnectable()) {
                        completeConnection(key, (SocketChannel) key.channel(), (InetSocketAddress) key.attachment());
                    } else if (key.isReadable()) {
                        SocketChannel ch = (SocketChannel) key.channel();
                        InetSocketAddress remoteAddress = channelToAddress.get(ch);

                        ByteBuffer readBuffer = ByteBuffer.allocate(MAX_READ_SIZE);
                        int bytesRead = ch.read(readBuffer);
                        readBuffer.flip();

                        if (bytesRead == -1) {
                            ch.close();
                            key.cancel();
                            sockets.remove(remoteAddress);
                            channelToAddress.remove(ch);
                            continue;
                        }

                        try {
                            if (readBuffer.hasRemaining()) {
                                long sessionId = readBuffer.getLong();
                                long blockId = readBuffer.getLong();
                                Object o = read(readBuffer);
                                Session session = Main.bootstrap.getSessionManager().getSession(sessionId);
                                String varName = session.getSessionData().getPreparedBlock().get(blockId);

                                session.getSessionData().assignVariable(varName, o);

                            }
                        } catch (KException e) {
                            // Error handling here, without printing to System.err
                        }
                    }
                }
            } catch (IOException e) {
                // Error handling here, without printing to System.err
            }
        }
    }
}