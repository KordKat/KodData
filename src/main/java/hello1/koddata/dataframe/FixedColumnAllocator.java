package hello1.koddata.dataframe;

import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.memory.Memory;
import hello1.koddata.memory.MemoryUtil;
import hello1.koddata.memory.ReadOnlyMemory;
import sun.misc.Unsafe;
import java.nio.ByteBuffer;

public class FixedColumnAllocator extends ColumnAllocator {
    private ByteBuffer dataBuffer;
    private boolean[] notNullFlags;

    public FixedColumnAllocator(ByteBuffer dataBuffer, boolean[] notNullFlags, int elementSize) throws KException {
        super(notNullFlags.length, elementSize);
        this.dataBuffer = dataBuffer;
        this.notNullFlags = notNullFlags;
        if (super.isVariable()) {
            throw new KException(ExceptionCode.KD00005, "FixedColumnAllocator only supports fixed types");
        }
    }

    @Override
    public Memory allocate() throws KException {
        byte[] nullBitmap = buildNullBitmap(notNullFlags);
        long nullBytes = nullBitmap.length;

        int dataCount = 0;
        for (boolean b : notNullFlags) {
            if (b) dataCount++;
        }

        int dataBytes = dataCount * super.sizePerElement;
        int totalBytes = (int) (nullBytes + dataBytes);

        byte[] combined = new byte[totalBytes];

        System.arraycopy(nullBitmap, 0, combined, 0, (int) nullBytes);

        int writePos = (int) nullBytes;

        if (dataBuffer.isDirect()) {
            long base = MemoryUtil.getByteBufferAddress(dataBuffer);
            int offset = dataBuffer.position();
            Unsafe unsafe = MemoryUtil.unsafe;

            for (int i = 0; i < rows; i++) {
                long srcAddr = base + offset;
                if (notNullFlags[i]) {
                    unsafe.copyMemory(null, srcAddr, combined, Unsafe.ARRAY_BYTE_BASE_OFFSET + writePos, super.sizePerElement);
                    writePos += super.sizePerElement;
                    offset += super.sizePerElement;
                }
            }

        } else if (dataBuffer.hasArray()) {
            byte[] arr = dataBuffer.array();
            int arrayOffset = dataBuffer.arrayOffset() + dataBuffer.position();

            for (int i = 0; i < rows; i++) {
                if (notNullFlags[i]) {
                    System.arraycopy(arr, arrayOffset + (i * super.sizePerElement),
                            combined, writePos, super.sizePerElement);
                    writePos += super.sizePerElement;
                }
            }

        } else {
            throw new KException(ExceptionCode.KD00006,
                    "Unsupported ByteBuffer type (must be direct or array-backed)");
        }

        ReadOnlyMemory mem = ReadOnlyMemory.allocate(totalBytes);
        mem.initData(combined);
        dataBuffer = null;
        notNullFlags = null;
        System.gc();
        return mem;
    }

    // [null bitmap] [data]
}
