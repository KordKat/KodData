package hello1.koddata.io;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

//Inheritance
public abstract class DataInputStreamPromax extends InputStream implements DataInputPromax {

    //Polymorphism
    @Override
    public int read(byte[] buffer) throws IOException {
        Objects.requireNonNull(buffer, "array must not be null (read(byte[]))");
        return read(buffer, 0, buffer.length);
    }

    //Polymorphism
    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
        Objects.requireNonNull(buffer, "array must not be null (read(byte[], off, len))");
        if (offset < 0 || length < 0 || offset + length > buffer.length) {
            throw new IndexOutOfBoundsException("Offset and length out of bounds.");
        }
        if (length == 0) return 0;

        for (int i = 0; i < length; i++) {
            int r = read();
            if (r < 0) return (i == 0) ? -1 : i;
            buffer[offset + i] = (byte) r;
        }
        return length;
    }
}