package hello1.koddata.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public class SocketFactory {

    private int writeBufferSize;
    private int readBufferSize;
    private boolean tcpNoDelay;
    private boolean reuseAddress;
    private boolean keepAlive;

    private SocketFactory(Builder builder){
        this.writeBufferSize = builder.writeBufferSize;
        this.readBufferSize = builder.readBufferSize;
        this.tcpNoDelay = builder.tcpNoDelay;
        this.reuseAddress = builder.reuseAddress;
        this.keepAlive = builder.keepAlive;
    }

    public SocketChannel createNode(String host, int port) throws IOException {
        SocketChannel channel = SocketChannel.open();
        channel.setOption(StandardSocketOptions.TCP_NODELAY, tcpNoDelay);
        channel.setOption(StandardSocketOptions.SO_REUSEADDR, reuseAddress);
        channel.setOption(StandardSocketOptions.SO_KEEPALIVE, keepAlive);

        if(writeBufferSize > 0)
            channel.setOption(StandardSocketOptions.SO_SNDBUF, writeBufferSize);
        if(readBufferSize > 0)
            channel.setOption(StandardSocketOptions.SO_RCVBUF, readBufferSize);

        channel.connect(new InetSocketAddress(host, port));
        channel.configureBlocking(false);
        return channel;
    }

    public ServerSocketChannel createServer(int port) throws IOException{
        ServerSocketChannel serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, reuseAddress);
        serverChannel.bind(new InetSocketAddress(port));
        return serverChannel;
    }

    public static class Builder {
        private int writeBufferSize = 64 * 1024;
        private int readBufferSize = 64 * 1024;
        private boolean tcpNoDelay = true;
        private boolean reuseAddress = true;
        private boolean keepAlive = true;

        public Builder sendBufferSize(int bytes) {
            this.writeBufferSize = bytes;
            return this;
        }

        public Builder readBufferSize(int bytes){
            this.readBufferSize = bytes;
            return this;
        }

        public Builder tcpNoDelay(boolean tcpNoDelay){
            this.tcpNoDelay = tcpNoDelay;
            return this;
        }

        public Builder reuseAddress(boolean reuseAddress){
            this.reuseAddress = reuseAddress;
            return this;
        }

        public Builder keepAlive(boolean keepAlive){
            this.keepAlive = keepAlive;
            return this;
        }

        public SocketFactory get(){
            return new SocketFactory(this);
        }

    }

}
