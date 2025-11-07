package hello1.koddata.io;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class BufferedOutputStreamPromax extends DataOutputStreamPromax{

    private final BufferedOutputStream out;

    public BufferedOutputStreamPromax(OutputStream os) {
        this.out = new BufferedOutputStream(os);
    }

    public BufferedOutputStreamPromax(OutputStream os, int bufferSize) {
        this.out = new BufferedOutputStream(os, bufferSize);
    }

    @Override
    public void write(int b) throws IOException {
        out.write(b);
    }

    @Override
    public void writeUTF(String s) throws IOException {
        new java.io.DataOutputStream(out).writeUTF(s);
    }

    

    @Override
    public void write(ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            out.write(buf.get() & 0xFF);
        }
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    @Override
    public void close() throws IOException {
        out.close();
    }
}