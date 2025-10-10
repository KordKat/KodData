package hello1.koddata.memory;

import sun.misc.Unsafe;

import java.io.DataOutput;
import java.lang.reflect.Field;

public abstract class Memory {

    protected long peer;
    protected long allocatedSize;

    protected Memory(long peer, long size){
        this.peer = peer;
        this.allocatedSize = size;
    }

    protected Memory(Memory copy){
        this.peer = copy.peer;
        this.allocatedSize = copy.allocatedSize;
    }

    static long getPeer(long bytes){
        if(bytes <= 0){
            throw new RuntimeException("Invalid number of bytes");
        }

        long peer = MemoryUtil.unsafe.allocateMemory(bytes);

        if(peer == 0){
            throw new RuntimeException("Cannot allocate memory");
        }

        return peer;
    }

    public void free(){
        MemoryUtil.unsafe.freeMemory(peer);
    }

    public void set(Memory memory, long size){
        checkBounds(size);
        memory.checkBounds(size);
        MemoryUtil.unsafe.copyMemory(memory.peer, peer, size);
    }

    public Memory copy(long newSize, boolean readOnly){
        Memory memory;
        if(readOnly){
            memory = ReadOnlyMemory.allocate(newSize);
        }else {
            memory = WritableMemory.allocate(newSize);
        }

        memory.set(this, Math.min(allocatedSize, newSize));
        return memory;
    }

    protected void checkBounds(long size){
        if(size > allocatedSize){
            free();
            throw new RuntimeException("Memory out of bounds");
        }
    }

    public int readInt(){
        if(allocatedSize < 4){
            free();
            throw new IllegalStateException("Invalid size to read");
        }

        return MemoryUtil.unsafe.getInt(peer);
    }

    public long readLong(){
        if(allocatedSize < 8){
            throw new IllegalStateException("Invalid size to read");
        }

        return MemoryUtil.unsafe.getLong(peer);
    }

    public byte[] readBytes(int count) {
        if (count < 0 || count > allocatedSize) {
            throw new IllegalArgumentException("Invalid byte count: " + count);
        }
        byte[] buf = new byte[count];
        MemoryUtil.unsafe.copyMemory(null, peer, buf, Unsafe.ARRAY_BYTE_BASE_OFFSET, count);
        return buf;
    }


}
