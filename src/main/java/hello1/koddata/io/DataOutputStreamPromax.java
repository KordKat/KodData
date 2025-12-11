package hello1.koddata.io;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

//Inheritance
//Abstract
public abstract class DataOutputStreamPromax extends OutputStream implements DataOutputPromax {

    //Polymorphism
    @Override
    public void write(byte[] b) throws IOException{
        Objects.requireNonNull(b,"array must not be null (write(byte[]))");
        write(b,0,b.length);
    }

    //Polymorphism
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        Objects.requireNonNull(b, "array must not be null (write(byte[], off, len))");
        if (off < 0 || len < 0 || off + len > b.length) {
            throw new IndexOutOfBoundsException("Offset and length out of bounds.");
        }
        for (int i = 0; i < len; i++) {
            write(b[off + i]);
        }
    }
}
