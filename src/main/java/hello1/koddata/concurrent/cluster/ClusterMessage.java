package hello1.koddata.concurrent.cluster;

import java.nio.channels.SocketChannel;
import hello1.koddata.net.NodeStatus;

public class ClusterMessage {

    public final int opcode;
    public final String resourceName;
    public final String payload;
    public final SocketChannel channel;
    public final NodeStatus sender;

    public ClusterMessage(
            int opcode,
            String resourceName,
            String payload,
            SocketChannel channel,
            NodeStatus sender
    ){
        this.opcode = opcode;
        this.resourceName = resourceName;
        this.payload = payload;
        this.channel = channel;
        this.sender = sender;
    }
}
