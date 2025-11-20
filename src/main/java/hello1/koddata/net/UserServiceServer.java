package hello1.koddata.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class UserServiceServer extends Server {

    private Selector selector;
    private ServerSocketChannel ssc;

    private ConcurrentMap<InetSocketAddress, SocketChannel> clients = new ConcurrentHashMap<>();

    private Thread serverThread;

    public UserServiceServer(InetSocketAddress inetSocketAddress, SocketFactory socketFactory) {
        super(inetSocketAddress, socketFactory);
    }

    @Override
    public void start() throws IOException {
        ssc = factory.createServer(inetSocketAddress.getPort());
        ssc.register(selector, SelectionKey.OP_ACCEPT);
        running = true;

        serverThread = new Thread(this::eventLoop);
    }

    public void acceptConnection(SelectionKey key) throws IOException {
        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
        SocketChannel ch = ssc.accept();

        if(ch != null){
            ch.configureBlocking(false);
            clients.put((InetSocketAddress) ch.getRemoteAddress(), ch);
            key.attach(new UserClient(selector, ch));
            serverSocketChannel.register(selector, SelectionKey.OP_READ);
        }
    }

    public void eventLoop(){
        while(running){

        }
    }

}
