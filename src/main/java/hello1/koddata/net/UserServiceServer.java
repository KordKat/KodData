package hello1.koddata.net;

import hello1.koddata.Main;
import hello1.koddata.exception.KException;
import hello1.koddata.sessions.Session;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class UserServiceServer extends Server {

    private Selector selector;
    private ServerSocketChannel ssc;

    private final ConcurrentMap<Long, UserUploadFileState> uploadFileStateMap = new ConcurrentHashMap<>();

    private final ConcurrentMap<InetSocketAddress, SocketChannel> clients = new ConcurrentHashMap<>();

    private Thread serverThread;

    public UserServiceServer(InetSocketAddress inetSocketAddress, SocketFactory socketFactory) {
        super(inetSocketAddress, socketFactory);
    }

    @Override
    public void start() throws IOException {
        selector = Selector.open();
        ssc = factory.createServer(inetSocketAddress.getPort());
        ssc.configureBlocking(false);
        ssc.register(selector, SelectionKey.OP_ACCEPT);
        running = true;

        serverThread = new Thread(this::eventLoop);
        serverThread.start();
    }

    public void acceptConnection(SelectionKey key) throws IOException {
        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
        SocketChannel ch = serverSocketChannel.accept();

        if(ch != null){
            ch.configureBlocking(false);
            clients.put((InetSocketAddress) ch.getRemoteAddress(), ch);
            UserClient client = new UserClient(selector, ch);
            ch.register(selector, SelectionKey.OP_READ, client);
        }
    }

    public void eventLoop(){
        while(running){
            try {
                if(selector.select() == 0){
                    continue;
                }

                Set<SelectionKey> selectedKeys = selector.selectedKeys();
                Iterator<SelectionKey> iterator = selectedKeys.iterator();

                while(iterator.hasNext()){
                    SelectionKey key = iterator.next();
                    iterator.remove();

                    if(!key.isValid()){
                        continue;
                    }

                    try {
                        if(key.isAcceptable()){
                            acceptConnection(key);
                        } else if(key.isReadable()){
                            handleRead(key);
                        } else if(key.isWritable()){
                            handleWrite(key);
                        }
                    } catch (IOException | KException e) {
                        handleDisconnect(key);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void handleRead(SelectionKey key) throws IOException, KException {
        SocketChannel channel = (SocketChannel) key.channel();
        UserClient client = (UserClient) key.attachment();
        ByteBuffer buffer = ByteBuffer.allocate(2048);

        int bytesRead = channel.read(buffer);
        if(bytesRead == -1){
            handleDisconnect(key);
            return;
        }

        if(bytesRead > 0){
            buffer.flip();
            byte mode = buffer.get();

            if (mode == 1) {
                UserUploadFileState state = uploadFileStateMap.get(client.getCurrentSession().id());
                if (state == null) {
                    int capacity = buffer.getInt();
                    state = new UserUploadFileState(capacity, client.getCurrentSession().id());
                    uploadFileStateMap.put(client.getCurrentSession().id(), state);

                    state.payloadLength = capacity - 4;
                    state.payloadBuffer.limit((int) state.payloadLength); // prevent over-write
                }

                int toRead = buffer.remaining();

                int remaining = Math.toIntExact(state.payloadLength - state.payloadBuffer.position());

                int chunk = Math.min(toRead, remaining);

                byte[] temp = new byte[chunk];
                buffer.get(temp);
                state.payloadBuffer.put(temp);

                if (!state.payloadBuffer.hasRemaining()) {

                    try {
                        state.perform();
                    } finally {
                        uploadFileStateMap.remove(client.getCurrentSession().id());
                    }
                }
            }else {
                byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes);
                String code = new String(bytes).trim();

                if (client != null && !code.isEmpty() && client.getCurrentSession() != null) {
                    client.executeCode(code);
                }
            }
        }
    }

    private void handleWrite(SelectionKey key) throws IOException {
        UserClient client = (UserClient) key.attachment();
        client.processWrite();
    }

    private void handleDisconnect(SelectionKey key) {
        try {
            SocketChannel ch = (SocketChannel) key.channel();
            if(ch != null && ch.getRemoteAddress() != null){
                clients.remove((InetSocketAddress) ch.getRemoteAddress());
                ch.close();
            }
            key.cancel();
        } catch (IOException ignored) {}
    }

}