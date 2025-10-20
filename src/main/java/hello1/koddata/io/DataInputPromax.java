package hello1.koddata.io;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;

public interface DataInputPromax extends DataInput {

    int read() throws IOException;
    int read(byte[] buffer, int offset, int length) throws IOException;

    @Override
    default void readFully(byte[] buffer) throws IOException {
        if (buffer == null) throw new NullPointerException("buffer must not be null (readFully)");
        readFully(buffer, 0, buffer.length);
    }

    @Override
    default void readFully(byte[] buffer, int offset, int length) throws IOException {
        if (buffer == null) throw new NullPointerException("buffer must not be null (readFully)");
        if (offset < 0 || length < 0 || offset + length > buffer.length) {
            throw new IndexOutOfBoundsException(
                    String.format("Offset and length out of bounds.)", offset, length, buffer.length)
            );
        }
        for (int n = 0; n < length; ) {
            int r = read(buffer, offset + n, length - n);
            if (r < 0) throw new EOFException("reached end-of-stream before reading all bytes");
            n += r;
        }
    }

    @Override
    default int skipBytes(int n) throws IOException {
        if (n <= 0) return 0;
        int skipped = 0;
        for (; skipped < n; skipped++) {
            int r = read();
            if (r < 0) break;
        }
        return skipped;
    }

    @Override
    default boolean readBoolean() throws IOException {
        return readUnsignedByte() != 0;
    }

    @Override
    default byte readByte() throws IOException {
        int ch = read();
        if (ch < 0) throw new EOFException();
        return (byte) ch;
    }

    @Override
    default int readUnsignedByte() throws IOException {
        int ch = read();
        if (ch < 0) throw new EOFException();
        return ch;
    }

    @Override
    default short readShort() throws IOException {
        int a = readUnsignedByte();
        int b = readUnsignedByte();
        return (short) ((a << 8) | b);
    }

    @Override
    default int readUnsignedShort() throws IOException {
        int a = readUnsignedByte();
        int b = readUnsignedByte();
        return (a << 8) | b;
    }

    @Override
    default char readChar() throws IOException {
        int a = readUnsignedByte();
        int b = readUnsignedByte();
        return (char) ((a << 8) | b);
    }

    @Override
    default int readInt() throws IOException {
        int a = readUnsignedByte();
        int b = readUnsignedByte();
        int c = readUnsignedByte();
        int d = readUnsignedByte();
        return (a << 24) | (b << 16) | (c << 8) | d;
    }

    @Override
    default long readLong() throws IOException {
        return ((long) readUnsignedByte() << 56)|
                ((long) readUnsignedByte() << 48)|
                ((long) readUnsignedByte() << 40)|
                ((long) readUnsignedByte() << 32)|
                ((long) readUnsignedByte() << 24)|
                ((long) readUnsignedByte() << 16)|
                ((long) readUnsignedByte() <<  8)|
                ((long) readUnsignedByte());
    }
    @Override
    default float readFloat() throws IOException{
        return Float.intBitsToFloat(readInt());
    }

    @Override
    default double readDouble() throws IOException{
        return Double.longBitsToDouble(readLong());
    }
    @Override
    default String readLine() throws IOException{
        StringBuilder sb = new StringBuilder();
        int ch;
        boolean sawAny = false;
        for (;;){
            ch = read();
            if (ch < 0) break;
            sawAny = true;
            if (ch == '\n') break;
            if (ch == '\r') { read(); break;
            }
            sb.append((char) (ch & 0xFF));
        }
        if (!sawAny && ch < 0) return null;
        return sb.toString();
    }
}
