package hello1.koddata.net;

import java.io.IOException;
import java.net.InetSocketAddress;

public class CommandSendingServer extends Server {
    public CommandSendingServer(InetSocketAddress inetSocketAddress, SocketFactory socketFactory) {
        super(inetSocketAddress, socketFactory);
    }

    @Override
    public void start() throws IOException {
        super.start();
    }

    @Override
    public void stop() throws IOException {
        super.stop();
    }

    public void sendCommand(byte[] data, int sessionId){
        
    }

}
