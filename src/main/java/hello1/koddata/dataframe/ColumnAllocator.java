package hello1.koddata.dataframe;

import hello1.koddata.memory.Allocator;
import hello1.koddata.memory.Memory;
import hello1.koddata.memory.MemoryUtil;
import hello1.koddata.memory.ReadOnlyMemory;
import sun.misc.Unsafe;

public abstract class ColumnAllocator implements Allocator {

    protected final long rows;
    protected final int sizePerElement;
    protected final boolean isVariable;
    protected ColumnAllocator(long rows, int sizePerElement) {
        this.rows = rows;
        this.sizePerElement = sizePerElement;
        this.isVariable = sizePerElement <= 0;
    }

    public boolean isVariable() {
        return isVariable;
    }

    public int getSizePerElement() {
        return sizePerElement;
    }

    public long getRows() {
        return rows;
    }

    protected byte[] buildNullBitmap(boolean[] notNullFlags) {
        int len = (int) ((rows + 7) / 8);
        byte[] bitmap = new byte[len];
        for (int i = 0; i < rows; i++) {
            if (notNullFlags[i]) bitmap[i / 8] |= (byte) (1 << (i % 8));
        }
        return bitmap;
    }

    protected long writeNullBitmap(ReadOnlyMemory mem, byte[] bitmap) {
        Unsafe unsafe = MemoryUtil.unsafe;
        long ptr = mem.getPeer();
        unsafe.copyMemory(bitmap, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, ptr, bitmap.length);
        return ptr + bitmap.length;
    }

    @Override
    public void deallocate(Memory ref) {
        if(ref != null) ref.free();
    }
}
