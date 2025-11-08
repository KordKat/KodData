package hello1.koddata.memory;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;

public class MemoryUtil {


    public static final Unsafe unsafe;
    static final Class<?> DIRECT_BYTE_BUFFER_CLASS;
    static final long DIRECT_BYTE_BUFFER_ADDR_OFFSET;

    static {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
            Class<?> clazz = ByteBuffer.allocateDirect(0).getClass();
            DIRECT_BYTE_BUFFER_CLASS = clazz;
            DIRECT_BYTE_BUFFER_ADDR_OFFSET = unsafe.objectFieldOffset(Buffer.class.getDeclaredField("address"));
        }catch (Exception ex){
            throw new RuntimeException(ex);
        }
    }

}
