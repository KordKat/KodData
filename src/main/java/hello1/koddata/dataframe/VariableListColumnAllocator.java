package hello1.koddata.dataframe;

import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.memory.Memory;
import hello1.koddata.memory.ReadOnlyMemory;

import java.util.List;

public class VariableListColumnAllocator extends VariableColumnAllocator {
//do it
    private final List<List<VariableElement>> lists;
    private final List<boolean[]> perListNotNullFlags;
    private final boolean[] columnNotNullFlags;
    private final int rows;

    public VariableListColumnAllocator(List<List<VariableElement>> lists,
                                       List<boolean[]> perListNotNullFlags,
                                       boolean[] columnNotNullFlags) throws KException {
        super(null, columnNotNullFlags);
        this.lists = lists;
        this.perListNotNullFlags = perListNotNullFlags;
        this.columnNotNullFlags = columnNotNullFlags;
        this.rows = columnNotNullFlags.length;

        if (lists.size() != rows || perListNotNullFlags.size() != rows) {
            throw new KException(ExceptionCode.KD00005, "lists and perListNotNullFlags size must equal rows");
        }
    }

    @Override
    public Memory allocate() throws KException {
        int columnNullBitmapBytes = (rows + 7) / 8;
        long totalBytes = getTotalBytes(columnNullBitmapBytes);

        byte[] combined = new byte[(int) totalBytes];
        int pos = 0;

        byte[] columnNullBitmap = buildNullBitmap(columnNotNullFlags);
        System.arraycopy(columnNullBitmap, 0, combined, pos, columnNullBitmapBytes);
        pos += columnNullBitmapBytes;

        for (int i = 0; i < rows; i++) {
            if (!columnNotNullFlags[i]) continue;

            List<VariableElement> list = lists.get(i);
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
                if (!perListNulls[j]) continue;

                VariableElement elem = list.get(j);
                byte[] val = elem.value();

                int size = val.length;
                combined[pos++] = (byte) ((size >> 24) & 0xFF);
                combined[pos++] = (byte) ((size >> 16) & 0xFF);
                combined[pos++] = (byte) ((size >> 8) & 0xFF);
                combined[pos++] = (byte) (size & 0xFF);

                System.arraycopy(val, 0, combined, pos, size);
                pos += size;
            }
        }

        ReadOnlyMemory mem = ReadOnlyMemory.allocate(totalBytes);
        mem.initData(combined);
        return mem;
    }

    private long getTotalBytes(int columnNullBitmapBytes) {
        long totalBytes = columnNullBitmapBytes;

        for (int i = 0; i < rows; i++) {
            if (!columnNotNullFlags[i]) continue;

            int listSize = lists.get(i).size();
            int perListNullBitmapBytes = (listSize + 7) / 8;

            long dataBytesForList = 0;
            boolean[] perListNulls = perListNotNullFlags.get(i);

            List<VariableElement> list = lists.get(i);
            for (int j = 0; j < listSize; j++) {
                if (!perListNulls[j]) continue;
                VariableElement elem = list.get(j);
                dataBytesForList += 4 + elem.value().length;
            }

            totalBytes += perListNullBitmapBytes + 4 + dataBytesForList;
        }
        return totalBytes;
    }
    /// [null bitmap][[null bitmap per-array][size of array][[size of element][.....]]]
}

