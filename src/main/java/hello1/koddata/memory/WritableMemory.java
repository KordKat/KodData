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

    public void setData(long offset, byte[] bytes) {
        if (bytes == null) {
            throw new NullPointerException("Bytes array cannot be null");
        }
        checkBounds(offset, bytes.length);
        MemoryUtil.unsafe.copyMemory(bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, peer + offset, bytes.length);
    }

    public void setData(long offset, long value) {
        checkBounds(offset, 8);
        MemoryUtil.unsafe.putLong(peer + offset, value);
    }

    public void setData(long offset, int value) {
        checkBounds(offset, 4);
        MemoryUtil.unsafe.putInt(peer + offset, value);
    }

    public void setData(long offset, float value) {
        checkBounds(offset, 4);
        MemoryUtil.unsafe.putFloat(peer + offset, value);
    }

    public void setData(long offset, double value) {
        checkBounds(offset, 4);
        MemoryUtil.unsafe.putDouble(peer + offset, value);
    }

    public void setData(long offset, ByteBuffer buffer) {
        if (buffer == null) {
            throw new NullPointerException("Buffer cannot be null");
        } else if (buffer.remaining() == 0) {
            return;
        }

        checkBounds(offset, buffer.remaining());

        if (buffer.hasArray()) {
            MemoryUtil.unsafe.copyMemory(
                    buffer.array(),
                    Unsafe.ARRAY_BYTE_BASE_OFFSET + buffer.position(),
                    null,
                    peer + offset,
                    buffer.remaining()
            );
        } else if (buffer.isDirect()) {
            long bufferAddress = MemoryUtil.unsafe.getLong(buffer, MemoryUtil.DIRECT_BYTE_BUFFER_ADDR_OFFSET);
            MemoryUtil.unsafe.copyMemory(bufferAddress + buffer.position(), peer + offset, buffer.remaining());
        } else {
            throw new IllegalStateException("Cannot initialize data from non-direct, non-array buffer");
        }
    }


    public void setData(byte[] bytes) {
        setData(0, bytes);
    }

    public void setData(long value) {
        setData(0, value);
    }

    public void setData(int value) {
        setData(0, value);
    }

    public void setData(float value) {
        setData(0, value);
    }

    public void setData(double value) {
        setData(0, value);
    }

    public void setData(ByteBuffer buffer) {
        setData(0, buffer);
    }

}
