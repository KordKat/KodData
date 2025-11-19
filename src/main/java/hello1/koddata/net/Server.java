package hello1.koddata.net;

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
    protected volatile boolean running = false;

    public Server(InetSocketAddress inetSocketAddress, SocketFactory socketFactory){
        this.inetSocketAddress = inetSocketAddress;
        this.factory = socketFactory;
    }

    public void start(){

    }

    public void stopGracefully(){

    }


    public void stop(){

    }

}
