package hello1.koddata;

import hello1.koddata.memory.SafeMemory;
import hello1.koddata.memory.WritableMemory;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        WritableMemory memory = WritableMemory.allocate(10);
        byte[] b = {59, -53, -25, -98, 15, 0, 0, 0};
        memory.setData(b);
        long l = memory.readLong();
        System.out.println(l);
        memory.free();

        SafeMemory sf = SafeMemory.allocate(8);
        SafeMemory sf2 = sf.share();
        sf.setData(b);
        l = sf.readLong();
        System.out.println(l);
        sf.close();
        sf2.setData(2000L);
        l = sf2.readLong();
        System.out.println(l);
        sf2.close();

    }
}
