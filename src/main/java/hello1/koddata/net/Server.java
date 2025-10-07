package hello1.koddata.net;

import hello1.koddata.net.util.NodeConnectionData;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Server {

    private final SocketFactory factory;
    private final int port;
    private final List<NodeConnectionData> connectionDataList;
    private ServerSocketChannel serverChannel;
    private Set<Node> neighbors;
    private boolean isGateway = false;

    public Server(SocketFactory factory, int port, List<NodeConnectionData> connectionDataList) {
        this.factory = factory;
        this.port = port;
        this.connectionDataList = connectionDataList;
        this.neighbors = ConcurrentHashMap.newKeySet();
    }

    public void start() throws IOException {
        serverChannel = factory.createServer(port);
        for(NodeConnectionData connectionData : connectionDataList){
            try {
                SocketChannel socketChannel = factory.createNode(connectionData.host, connectionData.port);
                socketChannel.write(ByteBuffer.wrap("HELLO".getBytes(StandardCharsets.UTF_8)));
            } catch (IOException e) {
                //ignored
            }
        }
    }

    public void shutdown(){
        try {
            serverChannel.close();
        } catch (IOException e) {
            // ignored
        }
    }

    public boolean isGateway(){
        return isGateway;
    }

}
