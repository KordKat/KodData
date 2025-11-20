package hello1.koddata.io;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;

public class ChannelState {
    public ByteBuffer headerBuffer = ByteBuffer.allocate(24); // header size
    public ByteBuffer payloadBuffer;

    public boolean headerComplete = false;

    public long sessionId;
    public long blockId;
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
}

