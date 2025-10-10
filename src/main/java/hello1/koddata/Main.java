package hello1.koddata;

import hello1.koddata.memory.Memory;
import hello1.koddata.memory.WritableMemory;
import hello1.koddata.net.SocketFactory;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.Arrays;

public class Main {
    public static void main(String[] args) throws IOException {
        WritableMemory memory = WritableMemory.allocate(10);
        byte[] b = {59, -53, -25, -98, 15, 0, 0, 0};
        memory.setData(b);
        long l = memory.readLong();
        System.out.println(l);
        memory.free();
    }
}
