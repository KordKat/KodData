package hello1.koddata;

import hello1.koddata.memory.Memory;
import hello1.koddata.memory.WritableMemory;
import hello1.koddata.net.SocketFactory;

import java.io.IOException;
import java.nio.channels.SocketChannel;

public class Main {
    public static void main(String[] args) throws IOException {
        WritableMemory memory = WritableMemory.allocateWritable(10);
        memory.setData(9L);
        long l = memory.readLong();
        System.out.println(l);
        memory.free();
    }
}
