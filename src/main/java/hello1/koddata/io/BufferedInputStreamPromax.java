package hello1.koddata.io;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

//Inheritance
public class BufferedInputStreamPromax extends DataInputStreamPromax {
//    BufferedInputStream
    private final InputStream in;
    private final byte[] buffer;
    private int pos = 0;
    private int count = 0;

    public BufferedInputStreamPromax(InputStream in, int bufferSize) {
        this.in = in;
        this.buffer = new byte[bufferSize];
    }

    private synchronized int fill() throws IOException {
        pos = 0;
        count = in.read(buffer, 0, buffer.length);
        return count;
    }

    //Polymorphism
    @Override
    public synchronized int read() throws IOException {
        if (pos >= count) {
            if (fill() <= 0) return -1;
        }
        return buffer[pos++] & 0xFF;
    }

    //Polymorphism
    @Override
    public synchronized String readUTF() throws IOException {
        if (pos >= count) {
            if(fill() <= 0)
                return null;
        }
        byte[] bytes = new byte[count - pos];
        System.arraycopy(buffer, pos, bytes, 0, count - pos);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    //Polymorphism
    @Override
    public synchronized void close() throws IOException {
        in.close();
    }

}
