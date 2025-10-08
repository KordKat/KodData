package hello1.koddata.memory;

import sun.misc.Unsafe;

import java.nio.ByteBuffer;

public class ReadOnlyMemory extends Memory{

    private boolean isWrite;

    private ReadOnlyMemory(long peer, long size){
        super(peer, size);
        isWrite = false;
    }

    public static ReadOnlyMemory allocateReadOnly(long bytes){
        return new ReadOnlyMemory(Memory.allocate(bytes), bytes);
    }

    public void initData(byte[] bytes){
        checkIsWrite();
        checkBounds(bytes.length);
        MemoryUtil.unsafe.copyMemory(bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, peer, bytes.length);
    }

    public void initData(long value){
        checkIsWrite();
        checkBounds(8);

        MemoryUtil.unsafe.putLong(peer, value);
    }

    public void initData(int value){
        checkIsWrite();
        checkBounds(4);
        MemoryUtil.unsafe.putInt(peer, value);
    }

    public void initData(ByteBuffer buffer){
        checkIsWrite();
        if(buffer == null) {
            throw new NullPointerException();
        }else if(buffer.remaining() == 0){
            return;
        }

        checkBounds(buffer.remaining());
        if(buffer.hasArray()){
            initData(buffer.array());
        }else if(buffer.isDirect()){
            MemoryUtil.unsafe.copyMemory(MemoryUtil.unsafe.getLong(buffer, MemoryUtil.DIRECT_BYTE_BUFFER_ADDR_OFFSET) + buffer.position(), peer, buffer.remaining());
        }else throw new IllegalStateException("Cannot innit data");
    }

    @Override
    public void set(Memory memory, long size) {
        checkIsWrite();
        super.set(memory, size);
        isWrite = true;
    }

    private void checkIsWrite(){
        if(isWrite){
            throw new RuntimeException("Cannot write to read-only memory");
        }
    }


}
