package hello1.koddata.memory;

import sun.misc.Unsafe;

import java.nio.ByteBuffer;

public class WritableMemory extends Memory {
    
    protected WritableMemory(long peer, long size){
        super(peer, size);
    }

    public WritableMemory(WritableMemory copy) {
        super(copy);
    }

    public static WritableMemory allocate(long bytes){
        return new WritableMemory(Memory.getPeer(bytes), bytes);
    }

    public void setData(byte[] bytes){
        checkBounds(bytes.length);
        MemoryUtil.unsafe.copyMemory(bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, peer, bytes.length);
    }

    public void setData(long value){
        checkBounds(8);
        MemoryUtil.unsafe.putLong(peer, value);
    }

    public void setData(int value){
        checkBounds(4);
        MemoryUtil.unsafe.putInt(peer, value);
    }

    public void setData(ByteBuffer buffer){
        if(buffer == null) {
            throw new NullPointerException();
        }else if(buffer.remaining() == 0){
            return;
        }

        checkBounds(buffer.remaining());
        if(buffer.hasArray()){
            setData(buffer.array());
        }else if(buffer.isDirect()){
            MemoryUtil.unsafe.copyMemory(MemoryUtil.unsafe.getLong(buffer, MemoryUtil.DIRECT_BYTE_BUFFER_ADDR_OFFSET) + buffer.position(), peer, buffer.remaining());
        }else throw new IllegalStateException("Cannot innit data");
    }
}
