package hello1.koddata.dataframe;

import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.memory.Memory;
import hello1.koddata.memory.ReadOnlyMemory;
import hello1.koddata.memory.MemoryUtil;
import sun.misc.Unsafe;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class VariableColumnAllocator extends ColumnAllocator {

    private final List<String> values;
    private final boolean[] notNullFlags;

    public VariableColumnAllocator(List<String> values, boolean[] notNullFlags, ColumnMetaData.ColumnType type) throws KException {
        super(values.size(), type);
        if (!type.isVariable()) {
            throw new KException(ExceptionCode.KD00005,"VariableColumnAllocator only supports variable-length types");
        }
        this.values = values;
        this.notNullFlags = notNullFlags;
    }

    @Override
    public Memory allocate() {
        byte[] nullBitmap = buildNullBitmap(notNullFlags);
        long nullBytes = nullBitmap.length;

        long totalDataBytes = 0;
        for (int i = 0; i < rows; i++) {
            if (!notNullFlags[i]) continue;
            byte[] data = values.get(i).getBytes(StandardCharsets.UTF_8);
            totalDataBytes += 4 + data.length;
        }

        ReadOnlyMemory mem = ReadOnlyMemory.allocate(nullBytes + totalDataBytes);
        long ptr = writeNullBitmap(mem, nullBitmap);

        Unsafe unsafe = MemoryUtil.unsafe;
        for (int i = 0; i < rows; i++) {
            if (!notNullFlags[i]) continue;
            byte[] data = values.get(i).getBytes(StandardCharsets.UTF_8);
            unsafe.putInt(ptr, data.length);
            ptr += 4;
            unsafe.copyMemory(data, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, ptr, data.length);
            ptr += data.length;
        }

        return mem;
    }
}

