package hello1.koddata.dataframe;

import hello1.koddata.exception.KException;
import hello1.koddata.memory.Memory;
import hello1.koddata.memory.ReadOnlyMemory;

import java.util.List;

public class VariableColumnAllocator extends ColumnAllocator {
//do it
    private final List<VariableElement> values;
    private final boolean[] notNullFlags;

    public VariableColumnAllocator(List<VariableElement> values, boolean[] notNullFlags) {
        super(notNullFlags.length, 0);
        this.values = values;
        this.notNullFlags = notNullFlags;
    }

    @Override
    public Memory allocate() throws KException {
        byte[] nullBitmap = buildNullBitmap(notNullFlags);
        int nullBytes = nullBitmap.length;

        int totalDataBytes = 0;
        for (int i = 0; i < rows; i++) {
            if (!notNullFlags[i]) continue;
            byte[] data = values.get(i).value();
            // 4 byte for storing element size
            totalDataBytes += 4 + data.length;
        }

        int totalBytes = nullBytes + totalDataBytes;

        byte[] combined = new byte[totalBytes];

        System.arraycopy(nullBitmap, 0, combined, 0, nullBytes);

        int writePos = nullBytes;

        for (int i = 0; i < rows; i++) {
            if (!notNullFlags[i]) continue;

            byte[] data = values.get(i).value();
            //storing size of element
            combined[writePos++] = (byte) ((data.length >> 24) & 0xFF);
            combined[writePos++] = (byte) ((data.length >> 16) & 0xFF);
            combined[writePos++] = (byte) ((data.length >> 8) & 0xFF);
            combined[writePos++] = (byte) (data.length & 0xFF);
            //actual value
            System.arraycopy(data, 0, combined, writePos, data.length);
            writePos += data.length;
        }

        ReadOnlyMemory mem = ReadOnlyMemory.allocate(totalBytes);
        mem.initData(combined);

        return mem;
    }
}

