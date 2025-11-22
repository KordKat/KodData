package hello1.koddata.dataframe;

import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.memory.Memory;
import hello1.koddata.memory.ReadOnlyMemory;

import java.util.List;

public class FixedListColumnAllocator extends VariableColumnAllocator {
//do it
    private final List<List<byte[]>> lists;
    private final List<boolean[]> perListNotNullFlags;
    private final boolean[] columnNotNullFlags;
    private final int elementSize;
    private final int rows;

    public FixedListColumnAllocator(List<List<byte[]>> lists, List<boolean[]> perListNotNullFlags,
                                    boolean[] columnNotNullFlags, int elementSize) throws KException {
        super(null, columnNotNullFlags);
        this.lists = lists;
        this.perListNotNullFlags = perListNotNullFlags;
        this.columnNotNullFlags = columnNotNullFlags;
        this.elementSize = elementSize;
        this.rows = columnNotNullFlags.length;

        if (lists.size() != rows || perListNotNullFlags.size() != rows) {
            throw new KException(ExceptionCode.KD00005, "lists and perListNotNullFlags size must equal rows");
        }
    }

    @Override
    public Memory allocate() throws KException {
        int columnNullBitmapBytes = (rows + 7) / 8;
        int totalBytes = columnNullBitmapBytes;

        for (int i = 0; i < rows; i++) {
            if (!columnNotNullFlags[i]) {
                continue;
            }
            int listSize = lists.get(i).size();
            int perListNullBitmapBytes = (listSize + 7) / 8;
            totalBytes += perListNullBitmapBytes + 4 + (countNonNull(perListNotNullFlags.get(i)) * elementSize);
        }

        byte[] combined = new byte[totalBytes];
        int pos = 0;

        byte[] columnNullBitmap = buildNullBitmap(columnNotNullFlags);
        System.arraycopy(columnNullBitmap, 0, combined, pos, columnNullBitmapBytes);
        pos += columnNullBitmapBytes;

        for (int i = 0; i < rows; i++) {
            if (!columnNotNullFlags[i]) {
                continue;
            }

            List<byte[]> list = lists.get(i);
            boolean[] perListNulls = perListNotNullFlags.get(i);

            byte[] perListNullBitmap = buildNullBitmap(perListNulls);
            System.arraycopy(perListNullBitmap, 0, combined, pos, perListNullBitmap.length);
            pos += perListNullBitmap.length;

            int listSize = list.size();
            combined[pos++] = (byte) ((listSize >> 24) & 0xFF);
            combined[pos++] = (byte) ((listSize >> 16) & 0xFF);
            combined[pos++] = (byte) ((listSize >> 8) & 0xFF);
            combined[pos++] = (byte) (listSize & 0xFF);

            for (int j = 0; j < listSize; j++) {
                if (!perListNulls[j]) {
                    continue;
                }
                byte[] elementBytes = list.get(j);
                if (elementBytes.length != elementSize) {
                    throw new KException(ExceptionCode.KD00005, "Element size mismatch at row " + i + ", element " + j);
                }
                System.arraycopy(elementBytes, 0, combined, pos, elementSize);
                pos += elementSize;
            }
        }

        ReadOnlyMemory mem = ReadOnlyMemory.allocate(totalBytes);
        mem.initData(combined);
        return mem;
    }

    private int countNonNull(boolean[] flags) {
        int count = 0;
        for (boolean b : flags) {
            if (b) count++;
        }
        return count;
    }

    /// [null bitmap][[null bitmap per-array][size of array][......]]
}