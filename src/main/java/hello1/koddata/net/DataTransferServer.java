package hello1.koddata.net;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

import hello1.koddata.Main;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.io.ChannelState;
import hello1.koddata.io.ServerStateChannelState;
import hello1.koddata.io.SessionBlockChannelState;
import hello1.koddata.io.UserFileChannelState;
import hello1.koddata.sessions.SessionManager;
import hello1.koddata.sessions.users.UserManager;
import hello1.koddata.utils.Serializable;
import hello1.koddata.utils.ref.ClusterReference;
import hello1.koddata.utils.ref.ReplicatedResourceClusterReference;

public class DataTransferServer extends Server implements Runnable {

    private static final byte MODE_SESSION_BLOCK = 0x30;
    private static final byte MODE_USER_FILE = 0x31;
    private static final byte MODE_SERVER_STATE = 0x32;
    public static final byte MODE_SERVER_STATE_FEEDBACK = 0x33;

    // CHUNK PROTOCOL constants
    // header = 8 (totalSize) + 1 (mode) + 8 (messageId) + 4 (totalChunks) + 4 (chunkIndex) + 4 (chunkPayloadSize) = 29
    private static final int CHUNK_HEADER_SIZE = 29;
    private static final int DEFAULT_CHUNK_SIZE = 1024 * 1024; // 1 MiB per chunk (changeable)

    private final Path rootBufferPath = Main.bootstrap.getRootPath().resolve("buffers/");

    private Set<InetSocketAddress> peers;
    private ConcurrentMap<InetSocketAddress, SocketChannel> sockets;
    private ConcurrentMap<SocketChannel, InetSocketAddress> channelToAddress;
    private ConcurrentMap<SocketChannel, ChannelState> readStates;
    private ConcurrentMap<SocketChannel, Queue<ByteBuffer>> writeQueues;

    // assembly state per messageId
    private final ConcurrentMap<Long, MessageAssembly> assemblies = new ConcurrentHashMap<>();

    private Thread serverThread;
    private Selector selector;
    private ServerSocketChannel ssc;

    private final int chunkSize;

    public DataTransferServer(InetSocketAddress inetSocketAddress, SocketFactory socketFactory) {
        this(inetSocketAddress, socketFactory, DEFAULT_CHUNK_SIZE);
    }

    public DataTransferServer(InetSocketAddress inetSocketAddress, SocketFactory socketFactory, int chunkSize) {
        super(inetSocketAddress, socketFactory);
        peers = new HashSet<>();
        sockets = new ConcurrentHashMap<>();
        channelToAddress = new ConcurrentHashMap<>();
        readStates = new ConcurrentHashMap<>();
        writeQueues = new ConcurrentHashMap<>();
        this.chunkSize = chunkSize;
    }

    @Override
    public void start() throws IOException {

        selector = Selector.open();
        ssc = factory.createServer(inetSocketAddress.getPort());
        ssc.register(selector, SelectionKey.OP_ACCEPT);

        // empty old buffer
        if (!Files.exists(rootBufferPath)) {
            Files.createDirectories(rootBufferPath);
        } else {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(rootBufferPath)) {
                for (Path entry : stream) {
                    Files.deleteIfExists(entry);
                }
            }
        }

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

    private void sendChunkedPayload(SocketChannel ch, byte mode, byte[] fullPayload) throws IOException {
        long totalSize = fullPayload.length;
        int csize = Math.max(1, chunkSize);
        int totalChunks = (int) ((totalSize + csize - 1) / csize);
        long messageId = UUID.randomUUID().getMostSignificantBits(); // could be negative but fine; use as unique id

        int offset = 0;
        for (int chunkIndex = 0; chunkIndex < totalChunks; chunkIndex++) {
            int thisChunkSize = Math.min(csize, fullPayload.length - offset);
            int headerAndPayloadSize = CHUNK_HEADER_SIZE + thisChunkSize;
            ByteBuffer buf = ByteBuffer.allocate(headerAndPayloadSize);

            buf.putLong(totalSize); // 8
            buf.put(mode); // 1
            buf.putLong(messageId); // 8
            buf.putInt(totalChunks); // 4
            buf.putInt(chunkIndex); // 4
            buf.putInt(thisChunkSize); // 4
            buf.put(fullPayload, offset, thisChunkSize);
            buf.flip();
            queueWrite(ch, buf);

            offset += thisChunkSize;
        }
    }

    public void transfer(InetSocketAddress node, Serializable serializable, long sessionId, String name, String index) throws KException {
        SocketChannel ch = sockets.get(node);
        if (ch == null || !ch.isConnected()) {
            throw new KException(ExceptionCode.KDN0011, "Peer node is not connected: " + node);
        }
        try {
            byte[] data = serializable.serialize();
            byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
            byte[] indexBytes = index == null ? new byte[0] : index.getBytes(StandardCharsets.UTF_8);

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            bos.write(ByteBuffer.allocate(8).putLong(sessionId).array());
            bos.write(ByteBuffer.allocate(4).putInt(nameBytes.length).array());
            bos.write(nameBytes);
            bos.write(ByteBuffer.allocate(4).putInt(indexBytes.length).array());
            bos.write(indexBytes);
            Integer wireId = Serializable.searchWireId(serializable.getClass());
            if (wireId == null) {
                throw new KException(ExceptionCode.KD00000, "Wire ID not registered for class: " + serializable.getClass().getName());
            }
            bos.write(ByteBuffer.allocate(4).putInt(wireId).array());
            bos.write(data);

            byte[] payload = bos.toByteArray();
            sendChunkedPayload(ch, MODE_SESSION_BLOCK, payload);
        } catch (IOException e) {
            throw new KException(ExceptionCode.KD00000, e.getMessage());
        }
    }

    public void transfer(InetSocketAddress node, Serializable serializable, long userId, String fileName) throws KException {
        SocketChannel ch = sockets.get(node);
        if (ch == null || !ch.isConnected()) {
            throw new KException(ExceptionCode.KDN0011, "Peer node is not connected: " + node);
        }

        try {
            byte[] data = serializable.serialize();
            byte[] fName = fileName.getBytes(StandardCharsets.UTF_8);

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            bos.write(ByteBuffer.allocate(8).putLong(userId).array());
            bos.write(ByteBuffer.allocate(4).putInt(fName.length).array());
            bos.write(fName);
            bos.write(data);

            byte[] payload = bos.toByteArray();
            sendChunkedPayload(ch, MODE_USER_FILE, payload);
        } catch (IOException e) {
            throw new KException(ExceptionCode.KD00000, e.getMessage());
        }
    }

    // sending node replicated resource for consistency (now chunked)
    public void transfer(InetSocketAddress node, ConcurrentMap<String, ReplicatedResourceClusterReference<?>> resources) throws KException, IOException {
        SocketChannel ch = sockets.get(node);
        if (ch == null || !ch.isConnected()) {
            throw new KException(ExceptionCode.KDN0011, "Peer node is not connected: " + node);
        }

        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
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
            sendChunkedPayload(ch, MODE_SERVER_STATE, dataset);
        } catch (IOException e) {
            throw new KException(ExceptionCode.KD00000, e.getMessage());
        }
    }

    // feedback when data is inconsistent (feedback is small; send single chunk)
    public void sendFeedback(SocketChannel ch, byte[] data) {
        try {
            sendChunkedPayload(ch, MODE_SERVER_STATE_FEEDBACK, data);
        } catch (IOException ignored) {}
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
                // swallow, allow loop to continue if running
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

    // ---------- RECEIVING: now chunk-aware ----------
    private void handleRead(SelectionKey key) {
        SocketChannel ch = (SocketChannel) key.channel();
        ChannelState state = readStates.get(ch);

        try {
            if (state == null) {
                // read chunk header first (29 bytes)
                ByteBuffer headerBuf = ByteBuffer.allocate(CHUNK_HEADER_SIZE);
                int read = ch.read(headerBuf);
                if (read == -1) {
                    closeChannel(ch, key);
                    return;
                }
                if (read < CHUNK_HEADER_SIZE) {
                    // partial header read: create a temporary ChunkReceiveState that stores header progress
                    ChunkReceiveState crs = new ChunkReceiveState();
                    crs.headerBuffer = headerBuf;
                    crs.headerBuffer.position(read);
                    readStates.put(ch, crs);
                    return;
                }

                headerBuf.flip();
                long totalSize = headerBuf.getLong();
                byte mode = headerBuf.get();
                long messageId = headerBuf.getLong();
                int totalChunks = headerBuf.getInt();
                int chunkIndex = headerBuf.getInt();
                int chunkPayloadSize = headerBuf.getInt();

                // Create state to read the payload for this chunk
                ChunkReceiveState crs = new ChunkReceiveState();
                crs.totalSize = totalSize;
                crs.mode = mode;
                crs.messageId = messageId;
                crs.totalChunks = totalChunks;
                crs.chunkIndex = chunkIndex;
                crs.chunkPayloadSize = chunkPayloadSize;
                crs.payloadBuffer = ByteBuffer.allocate(chunkPayloadSize);
                crs.payloadLength = chunkPayloadSize;
                crs.bytesReceived = 0;

                readStates.put(ch, crs);
            } else {
                // if we have an existing state, it might be a header partial or payload partial
                if (state instanceof ChunkReceiveState) {
                    ChunkReceiveState crs = (ChunkReceiveState) state;

                    // If headerBuffer exists and hasn't been filled, continue reading header
                    if (crs.headerBuffer != null && crs.headerBuffer.hasRemaining()) {
                        int r = ch.read(crs.headerBuffer);
                        if (r == -1) {
                            closeChannel(ch, key);
                            return;
                        }
                        if (crs.headerBuffer.hasRemaining()) {
                            // still partial header
                            return;
                        } else {
                            // header finished, parse it and allocate payload buffer
                            crs.headerBuffer.flip();
                            crs.totalSize = crs.headerBuffer.getLong();
                            crs.mode = crs.headerBuffer.get();
                            crs.messageId = crs.headerBuffer.getLong();
                            crs.totalChunks = crs.headerBuffer.getInt();
                            crs.chunkIndex = crs.headerBuffer.getInt();
                            crs.chunkPayloadSize = crs.headerBuffer.getInt();
                            crs.payloadBuffer = ByteBuffer.allocate(crs.chunkPayloadSize);
                            crs.payloadLength = crs.chunkPayloadSize;
                            crs.bytesReceived = 0;
                            crs.headerBuffer = null; // no longer needed
                        }
                    }

                    // Now read payload portion
                    if (crs.payloadBuffer != null && crs.payloadBuffer.hasRemaining()) {
                        int bytesRead = ch.read(crs.payloadBuffer);
                        if (bytesRead == -1) {
                            closeChannel(ch, key);
                            return;
                        }
                        crs.bytesReceived += Math.max(0, bytesRead);
                    }

                    if (crs.payloadBuffer != null && !crs.payloadBuffer.hasRemaining()) {
                        // payload completed for this chunk; persist chunk and update assembly
                        crs.payloadBuffer.flip();
                        saveChunkAndMaybeAssemble(crs);
                        // remove state so next read starts header for next chunk
                        readStates.remove(ch);
                    }
                } else {
                    // existing non-chunk state: fallback (shouldn't happen in new protocol)
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
                }
            }
        } catch (Exception e) {
            // on error, close channel and remove partial states
            closeChannel(ch, key);
        }
    }

    // Save the chunk bytes to disk and, if message complete, assemble and dispatch
    private void saveChunkAndMaybeAssemble(ChunkReceiveState crs) {
        try {
            long messageId = crs.messageId;
            MessageAssembly assembly = assemblies.computeIfAbsent(messageId, id -> {
                try {
                    return new MessageAssembly(id, crs.mode, crs.totalChunks, crs.totalSize);
                } catch (IOException e) {
                    return null;
                }
            });

            if (assembly == null) return;

            byte[] chunkBytes = new byte[crs.chunkPayloadSize];
            crs.payloadBuffer.get(chunkBytes);

            assembly.storeChunk(crs.chunkIndex, chunkBytes);

            if (assembly.isComplete()) {
                // assemble final bytes (stream to avoid one huge allocation if extremely large)
                Path assembledPath = assembly.assembleToFile(rootBufferPath);
                // after assembling, dispatch according to mode
                byte mode = assembly.mode;
                if (mode == MODE_USER_FILE) {
                    // read assembled data into a UserFileChannelState and call perform()
                    byte[] all = Files.readAllBytes(assembledPath);
                    // create channel state and simulate perform()
                    UserFileChannelState ucs = new UserFileChannelState((int) assembly.totalSize, Main.bootstrap.getUserManager());
                    ucs.payloadLength = (int) all.length;
                    ucs.payloadBuffer = ByteBuffer.wrap(all);
                    ucs.bytesReceived = all.length;
                    ucs.perform();
                } else if (mode == MODE_SESSION_BLOCK) {
                    byte[] all = Files.readAllBytes(assembledPath);
                    SessionBlockChannelState scs = new SessionBlockChannelState((int) assembly.totalSize, Main.bootstrap.getSessionManager());
                    scs.payloadLength = (int) all.length;
                    scs.payloadBuffer = ByteBuffer.wrap(all);
                    scs.bytesReceived = all.length;
                    scs.perform();
                } else if (mode == MODE_SERVER_STATE || mode == MODE_SERVER_STATE_FEEDBACK) {
                    byte[] all = Files.readAllBytes(assembledPath);
                    ServerStateChannelState ss = new ServerStateChannelState((int) assembly.totalSize, this, null, mode == MODE_SERVER_STATE_FEEDBACK);
                    ss.payloadLength = (int) all.length;
                    ss.payloadBuffer = ByteBuffer.wrap(all);
                    ss.bytesReceived = all.length;
                    ss.perform();
                }

                // cleanup assembly and chunk files
                assemblies.remove(messageId);
                assembly.cleanup();
            }

        } catch (Exception ignored) {
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

    // ----------------- helper classes -----------------

    /**
     * Temporary state used while reading a single chunk.
     */
    private static class ChunkReceiveState extends ChannelState {
        // used for partial header reads
        private ByteBuffer headerBuffer;

        // parsed header fields
        private long totalSize;
        private byte mode;
        private long messageId;
        private int totalChunks;
        private int chunkIndex;
        private int chunkPayloadSize;

        public ChunkReceiveState() {
            super(0);
            this.headerBuffer = ByteBuffer.allocate(CHUNK_HEADER_SIZE);
        }

        @Override
        public void perform() throws KException {

        }
    }

    /**
     * Tracks the assembly of a multi-chunk message and stores chunks on disk
     * under rootBufferPath/<messageId>/
     */
    private static class MessageAssembly {
        final long messageId;
        final byte mode;
        final int totalChunks;
        final long totalSize;
        final Path dir;
        final BitSet received;
        final Object lock = new Object();

        MessageAssembly(long messageId, byte mode, int totalChunks, long totalSize) throws IOException {
            this.messageId = messageId;
            this.mode = mode;
            this.totalChunks = totalChunks;
            this.totalSize = totalSize;
            this.dir = Main.bootstrap.getRootPath().resolve("buffers").resolve(String.valueOf(messageId));
            if (!Files.exists(this.dir)) Files.createDirectories(this.dir);
            this.received = new BitSet(totalChunks);
        }

        void storeChunk(int index, byte[] bytes) throws IOException {
            Path chunkPath = dir.resolve(String.format("chunk_%05d.part", index));
            try (OutputStream os = Files.newOutputStream(chunkPath, java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.TRUNCATE_EXISTING)) {
                os.write(bytes);
            }
            synchronized (lock) {
                received.set(index);
            }
        }

        boolean isComplete() {
            synchronized (lock) {
                return received.cardinality() == totalChunks;
            }
        }

        /**
         * Assemble into a single file under the same dir called "assembled.dat".
         * Returns assembled path.
         */
        Path assembleToFile(Path rootBufferPath) throws IOException {
            Path assembled = dir.resolve("assembled.dat");
            try (OutputStream os = Files.newOutputStream(assembled, java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.TRUNCATE_EXISTING)) {
                for (int i = 0; i < totalChunks; i++) {
                    Path chunk = dir.resolve(String.format("chunk_%05d.part", i));
                    try (InputStream is = Files.newInputStream(chunk)) {
                        byte[] buf = new byte[8192];
                        int r;
                        while ((r = is.read(buf)) != -1) {
                            os.write(buf, 0, r);
                        }
                    }
                }
            }
            return assembled;
        }

        void cleanup() {
            try {
                // delete chunk files and assembled file and directory
                try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir)) {
                    for (Path p : ds) {
                        Files.deleteIfExists(p);
                    }
                }
                Files.deleteIfExists(dir);
            } catch (IOException ignored) {}
        }
    }
}
