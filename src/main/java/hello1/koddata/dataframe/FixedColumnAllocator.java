package hello1.koddata.dataframe;

import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.memory.Memory;
import hello1.koddata.memory.MemoryUtil;
import hello1.koddata.memory.ReadOnlyMemory;
import sun.misc.Unsafe;

import java.nio.ByteBuffer;

public class FixedColumnAllocator extends ColumnAllocator {
    private final ByteBuffer dataBuffer;
    private final boolean[] notNullFlags;

    public FixedColumnAllocator(ByteBuffer dataBuffer, boolean[] notNullFlags, ColumnMetaData.ColumnType type) throws KException {
        super(notNullFlags.length, type);
        this.dataBuffer = dataBuffer;
        this.notNullFlags = notNullFlags;
        if (dataBuffer.remaining() != type.size() * notNullFlags.length) {
            throw new KException(ExceptionCode.KD00005, "Data buffer size does not match expected size");
        }
        if (type.isVariable()) {
            throw new KException(ExceptionCode.KD00005, "FixedColumnAllocator only supports fixed types");
        }
    }

    @Override
    public Memory allocate() throws KException {
        byte[] nullBitmap = buildNullBitmap(notNullFlags);
        long nullBytes = nullBitmap.length;
        long dataCount = 0;
        for (boolean b : notNullFlags) if (b) dataCount++;
        long dataBytes = dataCount * type.size();

        ReadOnlyMemory mem = ReadOnlyMemory.allocate(nullBytes + dataBytes);
        long ptr = writeNullBitmap(mem, nullBitmap);

        Unsafe unsafe = MemoryUtil.unsafe;
        for (int i = 0; i < rows; i++) {
            if (!notNullFlags[i]) {
                dataBuffer.position(dataBuffer.position() + type.size());
                continue;
            }
            switch (type) {
                case INT -> {
                    int val = dataBuffer.getInt();
                    unsafe.putInt(ptr, val);
                    ptr += 4;
                }
                case LONG -> {
                    long val = dataBuffer.getLong();
                    unsafe.putLong(ptr, val);
                    ptr += 8;
                }
                case FLOAT -> {
                    float val = dataBuffer.getFloat();
                    unsafe.putFloat(ptr, val);
                    ptr += 4;
                }
                case DOUBLE -> {
                    double val = dataBuffer.getDouble();
                    unsafe.putDouble(ptr, val);
                    ptr += 8;
                }
                case BYTE, BOOLEAN -> {
                    byte val = dataBuffer.get();
                    unsafe.putByte(ptr, val);
                    ptr += 1;
                }
                default -> throw new KException(ExceptionCode.KD00006, "Unsupported fixed column type " + type);
            }
        }
        return mem;
    }
}
