package hello1.koddata.io;

import java.io.DataOutput;
import java.io.IOException;

public interface DataOutputPromax extends DataOutput {
    @Override
    default void writeBoolean(boolean b) throws IOException {
        write((int)(b ? 1 : 0));
    }

    @Override
    default void writeByte(int i) throws IOException{

    }
}
