package hello1.koddata.memory;

import sun.misc.Unsafe;

import java.io.DataOutput;
import java.lang.reflect.Field;

public class Memory {

    private static final Unsafe unsafe;
    private long peer;
    static {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
        }catch (Exception ex){
            throw new RuntimeException(ex);
        }
    }
    private Memory(long peer){
        this.peer = peer;
    }

    public static Memory allocate(long size){
        if(size <= 0){
            throw new RuntimeException("Invalid size: " + size);
        }
        long peer = unsafe.allocateMemory(size);
        if(peer == 0){
            throw new RuntimeException("Cannot allocate memory");
        }

        return new Memory(peer);
    }

}
