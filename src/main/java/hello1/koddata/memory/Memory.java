package hello1.koddata.memory;

import sun.misc.Unsafe;

import java.io.DataOutput;
import java.lang.reflect.Field;
import java.util.Objects;

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

    public long getPeer() {
        return peer;
    }

    static long resizeMemory(long oldPeer, long oldSize, long newSize){
        if(newSize <= 0){
            throw new RuntimeException("Invalid number of bytes");
        }
        long peer = MemoryUtil.unsafe.allocateMemory(newSize);
        long copySize = Math.min(oldSize, newSize);
        MemoryUtil.unsafe.copyMemory(oldPeer, peer, copySize);
        MemoryUtil.unsafe.freeMemory(oldPeer);
        return peer;
    }

    public void resize(long newSize){
        this.peer = resizeMemory(peer, allocatedSize, newSize);
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

    protected void checkBounds(long offset, long size) {
        if (offset < 0 || size < 0 || offset + size > allocatedSize) {
            throw new IllegalArgumentException(
                    "Out of bounds: offset=" + offset + ", size=" + size + ", allocatedSize=" + allocatedSize
            );
        }
    }

    public long size(){
        return allocatedSize;
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

    public int readInt(long offset) {
        if (offset < 0 || offset + 4 > allocatedSize) {
            throw new IllegalArgumentException("Invalid offset to read int: " + offset);
        }
        return MemoryUtil.unsafe.getInt(peer + offset);
    }

    public short readShort(long offset) {
        if (offset < 0 || offset + 4 > allocatedSize) {
            throw new IllegalArgumentException("Invalid offset to read int: " + offset);
        }
        return MemoryUtil.unsafe.getShort(peer + offset);
    }

    public long readLong(long offset) {
        if (offset < 0 || offset + 8 > allocatedSize) {
            throw new IllegalArgumentException("Invalid offset to read long: " + offset);
        }
        return MemoryUtil.unsafe.getLong(peer + offset);
    }

    public float readFloat(long offset) {
        if (offset < 0 || offset + 4 > allocatedSize) {
            throw new IllegalArgumentException("Invalid offset to read long: " + offset);
        }
        return MemoryUtil.unsafe.getFloat(peer + offset);
    }

    public double readDouble(long offset) {
        if (offset < 0 || offset + 8 > allocatedSize) {
            throw new IllegalArgumentException("Invalid offset to read long: " + offset);
        }
        return MemoryUtil.unsafe.getDouble(peer + offset);
    }

    public byte[] readBytes(long offset, int count) {
        if (offset < 0 || count < 0 || offset + count > allocatedSize) {
            throw new IllegalArgumentException(
                    "Invalid offset/count (offset=" + offset + ", count=" + count + ", size=" + allocatedSize + ")"
            );
        }
        byte[] buf = new byte[count];
        MemoryUtil.unsafe.copyMemory(null, peer + offset, buf, Unsafe.ARRAY_BYTE_BASE_OFFSET, count);
        return buf;
    }


    @Override
    public int hashCode() {
        return Objects.hash(peer);
    }
}
