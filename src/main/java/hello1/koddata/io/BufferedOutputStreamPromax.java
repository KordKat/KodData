package hello1.koddata.io;

import jdk.internal.misc.InternalLock;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;



public class BufferedOutputStreamPromax extends DataOutputStreamPromax {
    private final InternalLock lock;
    private final OutputStream out;
    private byte[] buffer;
    private int count = 0;

    public BufferedOutputStreamPromax(OutputStream outputStream, InternalLock lock, OutputStream out, int bufferSize){
        this.lock = lock;
        this.out = out;
        this.buffer = new byte[bufferSize];
    }

    public BufferedOutputStreamPromax(OutputStream outputStream, InternalLock lock, OutputStream out){
        this.lock = lock;
        this.out = out;
    }
    @Override
    public synchronized void write(ByteBuffer buf) throws IOException {
        int remaining = buf.remaining();
        if (remaining == 0) return;

        if (buf.hasArray()) {
            int pos = buf.position();
            write(buf.array(), buf.arrayOffset() + pos, remaining);
            buf.position(pos + remaining);
        } else {
            while (buf.hasRemaining()) {
                if (count >= buffer.length) flushBuffer();
                int n = Math.min(buffer.length - count, buf.remaining());
                buf.get(buffer, count, n);
                count += n;
            }
        }
    }

    @Override
    public synchronized void write(byte[] b, int off, int len) throws IOException {
        if (len >= buffer.length) {
            flushBuffer();
            out.write(b, off, len);
            return;
        }

        if (len > buffer.length - count) {
            flushBuffer();
        }
        System.arraycopy(b, off, buffer, count, len);
        count += len;

    }


    @Override
    public void writeUTF(String s) throws IOException {
        new DataOutputStream(this).writeUTF(s);
    }


    @Override
    public synchronized void write(int b) throws IOException {
        if (count >= buffer.length) flushBuffer();
        buffer[count++] = (byte) b;
    }

    @Override
    public synchronized void flush() throws IOException {
        flushBuffer();
        out.flush();
    }
    public synchronized void close() throws IOException{
        flush();
        out.close();
    }
    private void flushBuffer() throws IOException {
        if (count > 0) {
            out.write(buffer, 0, count);
            count = 0;
        }
    }
}