package hello1.koddata.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public class Server {

    protected InetSocketAddress inetSocketAddress;
    protected SocketFactory factory;
    protected Selector selector;
    protected ServerSocketChannel serverChannel;
    protected SocketChannel channel;
    public volatile boolean running = false;

    public Server(InetSocketAddress inetSocketAddress, SocketFactory socketFactory){
        this.inetSocketAddress = inetSocketAddress;
        this.factory = socketFactory;
    }

    public void start() throws IOException {

    }

    public void stop() throws IOException {

    }

}
