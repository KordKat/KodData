package hello1.koddata.io;

import java.io.DataOutput;
import java.io.IOException;

public interface DataOutputPromax extends DataOutput {
    @Override
    default void writeBoolean(boolean b) throws IOException {
        write((int)(b ? 1 : 0));
    }

    @Override
    default void writeByte(int y) throws IOException{
        write(y);
    }

    @Override
    default void writeShort(int s) throws IOException{
        write(s >>> 8);
    }

    @Override
    default void writeChar(int c) throws IOException{
        write(c >>> 8);
    }

    @Override
    default void writeInt(int i) throws IOException{
        write(i >>> 24);
        write(i >>> 16);
        write(i >>> 8);
        write(i);
    }

    @Override
    default void writeLong(long l) throws IOException{
        write((int)l >>> 56);
        write((int)l >>> 48);
        write((int)l >>> 40);
        write((int)l >>> 32);
        write((int)l >>> 24);
        write((int)l >>> 16);
        write((int)l >>> 8);
        write((int)l);
    }

    @Override
    default void writeFloat(float f) throws IOException{
        writeInt(Float.floatToIntBits(f));
    }

    @Override
    default void writeDouble(double d) throws IOException {
        writeDouble(Double.doubleToLongBits(d));
    }

    @Override
    default void writeBytes(String s) throws IOException {
        if (s == null) throw new NullPointerException("s");
        for (int c : s.toCharArray())
            write(c);
    }

    @Override
    default void writeChars(String cs) throws IOException {
        if (cs == null) throw new NullPointerException("cs");
        for (int c : cs.toCharArray()) {
            write(c >>> 8);
            write(c);
        }
    }
}