package hello1.koddata.io;

import hello1.koddata.exception.KException;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;

//Abstract
public abstract class ChannelState {

    public ByteBuffer headerBuffer = ByteBuffer.allocate(24);
    public ByteBuffer payloadBuffer;

    public boolean headerComplete = false;

    public long payloadLength;
    public long bytesReceived;

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

    //Abstract
    public abstract void perform() throws KException;
}

