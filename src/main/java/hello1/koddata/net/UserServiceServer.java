package hello1.koddata.net;

import java.net.InetSocketAddress;

public class UserServiceServer extends Server {
    public UserServiceServer(InetSocketAddress inetSocketAddress, SocketFactory socketFactory) {
        super(inetSocketAddress, socketFactory);
    }
}
