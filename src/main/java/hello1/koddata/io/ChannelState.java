package hello1.koddata.io;

import hello1.koddata.exception.KException;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;

public abstract class ChannelState {
    private volatile long id;
    public ByteBuffer headerBuffer = ByteBuffer.allocate(24); // header size
    public ByteBuffer payloadBuffer;

    public boolean headerComplete = false;

    public long payloadLength;
    public long bytesReceived;

    public Path tempFile;
    public OutputStream output;

    public ChannelState(int chunkSize) {
        payloadBuffer = ByteBuffer.allocate(chunkSize);
    }

    public void reset() {
        headerComplete = false;
        bytesReceived = 0;
        payloadLength = 0;

        headerBuffer.clear();
        payloadBuffer.clear();
    }

    public abstract void perform() throws KException;

    public long getId() {
        return id;
    }
}

