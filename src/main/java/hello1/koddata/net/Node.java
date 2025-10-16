package hello1.koddata.net;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class Node {

    private SocketChannel channel;
    private ByteBuffer readBuffer;

    public Node(SocketChannel channel , ByteBuffer readBuffer){
        this.channel = channel;
        this.readBuffer = readBuffer;
    }

    public SocketChannel getChannel() {
        return channel;
    }

    public ByteBuffer getReadBuffer() {
        return readBuffer;
    }



}
