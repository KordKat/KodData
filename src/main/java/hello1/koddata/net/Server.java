package hello1.koddata.net;

import java.net.InetSocketAddress;

public class Server {

    private InetSocketAddress inetSocketAddress;
    private SocketFactory factory;

    public Server(InetSocketAddress inetSocketAddress, SocketFactory socketFactory){
        this.inetSocketAddress = inetSocketAddress;
        this.factory = socketFactory;
    }

    public void start(){

    }

}
