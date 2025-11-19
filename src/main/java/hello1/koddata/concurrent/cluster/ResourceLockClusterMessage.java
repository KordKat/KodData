package hello1.koddata.concurrent.cluster;

import java.nio.channels.SocketChannel;

public record ResourceLockClusterMessage(
        byte opcode,
        String resourceKey,
        long processId,
        SocketChannel channel) {

    public byte getOpcode() {
        return opcode;
    }

    public String getResourceKey() {
        return resourceKey;
    }

    public long getProcessId() {
        return processId;
    }

    public SocketChannel getChannel() {
        return channel;
    }
}