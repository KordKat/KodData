package hello1.koddata.net;

import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public class GossipServer extends Server {

    private Set<InetSocketAddress> peers;
    private Thread thread;
    private volatile boolean running = false;

    public GossipServer(InetSocketAddress inetSocketAddress, SocketFactory socketFactory, Properties properties) throws KException {
        super(inetSocketAddress, socketFactory);
        String peerString = properties.getProperty("peers", "");
        String[] peerList = peerString.split(",");
        peers = new HashSet<>();
        for(String peer : peerList){
            peers.add(convertToInetSocketAddress(peer.trim().toLowerCase()));
        }
        thread = new Thread(this::run);

        thread.setDaemon(true);
        
    }

    private InetSocketAddress convertToInetSocketAddress(String hostname) throws KException {
        String host;
        int port;

        int colonIndex = hostname.lastIndexOf(":");
        if(colonIndex == -1){
            throw new KException(ExceptionCode.KDN0011, "Invalid hostname " + hostname);
        }

        host = hostname.substring(0, colonIndex);
        String portStr = hostname.substring(colonIndex + 1);

        port = Integer.parseInt(portStr);
        return new InetSocketAddress(host, port);
    }

    @Override
    public void start() {
        running = true;
        thread.start();
    }

    public void run(){

    }

}
