package hello1.koddata.dataframe;

import hello1.koddata.concurrent.IdCounter;
import hello1.koddata.engine.NullValue;
import hello1.koddata.engine.Value;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.memory.Memory;
import hello1.koddata.memory.MemoryGroup;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

public class Column{
    private static IdCounter columnIdCounter = new IdCounter();
    private long id;
    private String memoryGroupName;
    private ColumnMetaData metaData;
    private Memory memory;
    private byte columnKind;
    private int sizePerElement;
    private int startIdx;
    private int endIdx;
    private int rows;
    public Column(){}

    public Column(String name, int sizePerElement, String memoryGroupName, ByteBuffer dataBuffer, boolean[] notNullFlags, int elementSize, int startIdx, int endIdx, ColumnMetaData.ColumnDType dType) throws KException {
        setupMetadata(name, sizePerElement, memoryGroupName, false, dType);
        id = columnIdCounter.next();
        if(MemoryGroup.get(memoryGroupName) == null){
            throw new KException(ExceptionCode.KDM0007, "Memory group '" + memoryGroupName + "' does not exist.");
        }
        memory = MemoryGroup.get(memoryGroupName)
                .allocate(new FixedColumnAllocator(dataBuffer, notNullFlags, elementSize));
        columnKind = 0;
        this.rows = notNullFlags.length;
        this.startIdx = startIdx;
        this.endIdx = endIdx;
    }

    public Column(String name, List<VariableElement> values, String memoryGroupName, boolean[] notNullFlags, int startIdx, int endIdx, ColumnMetaData.ColumnDType dType) throws KException {
        setupMetadata(name, sizePerElement, memoryGroupName, true, dType);
        id = columnIdCounter.next();

        memory = MemoryGroup.get(memoryGroupName)
                .allocate(new VariableColumnAllocator(values, notNullFlags));
        columnKind = 1;
        this.rows = notNullFlags.length;
        this.startIdx = startIdx;
        this.endIdx = endIdx;
    }

    public Column(String name, String memoryGroupName, List<List<byte[]>> lists, List<boolean[]> perListNotNullFlags,
                  boolean[] columnNotNullFlags, int elementSize, int startIdx, int endIdx, ColumnMetaData.ColumnDType dType) throws KException {
        setupMetadata(name, elementSize, memoryGroupName, true, dType);
        id = columnIdCounter.next();

        memory = MemoryGroup.get(memoryGroupName)
                .allocate(new FixedListColumnAllocator(lists,perListNotNullFlags,columnNotNullFlags,elementSize));
        columnKind = 2;
        this.rows = columnNotNullFlags.length;
        this.startIdx = startIdx;
        this.endIdx = endIdx;
    }

    public Column(String name , String memoryGroupName,List<List<VariableElement>> lists,
                  List<boolean[]> perListNotNullFlags,
                  boolean[] columnNotNullFlags, int startIdx, int endIdx, ColumnMetaData.ColumnDType dType) throws KException {
        setupMetadata(name, -1, memoryGroupName, true, dType);
        id = columnIdCounter.next();
        memory = MemoryGroup.get(memoryGroupName)
                .allocate(new VariableListColumnAllocator(lists,perListNotNullFlags,columnNotNullFlags));
        columnKind = 3;
        this.rows = columnNotNullFlags.length;
        this.startIdx = startIdx;
        this.endIdx = endIdx;

    }

    public void setupMetadata(String name, int sizePerElement, String memoryGroupName, boolean isVariable, ColumnMetaData.ColumnDType dType){
        metaData = new ColumnMetaData(name, isVariable, dType);
        this.sizePerElement = sizePerElement;
        this.memoryGroupName = memoryGroupName;

    }

    public ColumnMetaData getMetaData() {
        return metaData;
    }

    public int getSizePerElement() {
        return sizePerElement;
    }


    public String getMemoryGroupName() {
        return memoryGroupName;
    }
    public Memory getMemory() {
        return memory;
    }

    public Value<?> readRow(int index, long offset){

        int nullbitmapsize = ((rows + 7) / 8);
        byte[] nullbitmap = memory.readBytes(nullbitmapsize);
        boolean isNull = isNull(index, nullbitmap);
        if(isNull){
            return new NullValue(new Object());
        }
        Value<?> value = null;
        switch (columnKind){
            case 0 -> {
                if(getMetaData().getDType().equals(ColumnMetaData.ColumnDType.SCALAR_INT)){
                    byte[] b = memory.readBytes(offset, Integer.BYTES);
                    ByteBuffer buffer = ByteBuffer.wrap(b);
                    Number n = buffer.getInt();
                    value = new Value<>(n);
                } else if(getMetaData().getDType().equals(ColumnMetaData.ColumnDType.SCALAR_DOUBLE)){
                    byte[] b = memory.readBytes(offset, Double.BYTES);
                    ByteBuffer buffer = ByteBuffer.wrap(b);
                    Number n = buffer.getDouble();
                    value = new Value<>(n);
                } else if(getMetaData().getDType().equals(ColumnMetaData.ColumnDType.SCALAR_LOGICAL)){
                    byte[] b = memory.readBytes(offset, 1);
                    ByteBuffer buffer = ByteBuffer.wrap(b);
                    Boolean n = buffer.get() == 1;
                    value = new Value<>(n);
                }else if(getMetaData().getDType().equals(ColumnMetaData.ColumnDType.SCALAR_DATE)){
                    byte[] b = memory.readBytes(offset, 1);
                    ByteBuffer buffer = ByteBuffer.wrap(b);
                    long l = buffer.getLong();
                    value = new Value<>(new Date(l));
                }else if(getMetaData().getDType().equals(ColumnMetaData.ColumnDType.SCALAR_TIMESTAMP)){
                    byte[] b = memory.readBytes(offset, 1);
                    ByteBuffer buffer = ByteBuffer.wrap(b);
                    long l = buffer.getLong();
                    value = new Value<>(new Timestamp(l));
                }
            }
            case 1 -> {
                byte[] sizeBytes = memory.readBytes(offset, Integer.BYTES);
                ByteBuffer sizeBuffer = ByteBuffer.wrap(sizeBytes);
                int dataLength = sizeBuffer.getInt();

                long valueOffset = offset + Integer.BYTES;
                byte[] dataBytes = memory.readBytes(valueOffset, dataLength);


                if(getMetaData().getDType().equals(ColumnMetaData.ColumnDType.SCALAR_STRING)){
                    String s = new String(dataBytes);
                    value = new Value<>(s);
                } else {
                    value = new Value<>(dataBytes);
                }
            }
            case 2 ->{
                byte[] listSizeBytes = memory.readBytes(offset, Integer.BYTES);
                ByteBuffer listSizeBuffer = ByteBuffer.wrap(listSizeBytes);
                int listSize = listSizeBuffer.getInt();

                long currentOffset = offset + Integer.BYTES;

                int perListNullBitmapBytes = (listSize + 7) / 8;
                byte[] perListNullBitmap = memory.readBytes(currentOffset, perListNullBitmapBytes);
                currentOffset += perListNullBitmapBytes;

                List<Value<?>> listValues = new ArrayList<>();
                int elementSize = getSizePerElement();

                ColumnMetaData.ColumnDType listDType = getMetaData().getDType();

                for (int j = 0; j < listSize; j++) {
                    boolean isElementNotNull = (perListNullBitmap[j / 8] & (1 << (j % 8))) != 0;

                    if (!isElementNotNull) {
                        listValues.add(new NullValue(new Object()));
                        continue;
                    }

                    byte[] elementBytes = memory.readBytes(currentOffset, elementSize);
                    ByteBuffer elementBuffer = ByteBuffer.wrap(elementBytes);
                    Value<?> elementValue;

                    if (listDType.equals(ColumnMetaData.ColumnDType.LIST_INT)) {
                        elementValue = new Value<>(elementBuffer.getInt());
                    } else if (listDType.equals(ColumnMetaData.ColumnDType.LIST_DOUBLE)) {
                        elementValue = new Value<>(elementBuffer.getDouble());
                    } else if (listDType.equals(ColumnMetaData.ColumnDType.LIST_LOGICAL)) {
                        elementValue = new Value<>(elementBuffer.get() == 1);
                    } else if (listDType.equals(ColumnMetaData.ColumnDType.LIST_DATE) || listDType.equals(ColumnMetaData.ColumnDType.LIST_TIMESTAMP)) {
                        elementValue = (listDType.equals(ColumnMetaData.ColumnDType.LIST_DATE)) ?
                                new Value<>(new Date(elementBuffer.getLong())) :
                                new Value<>(new Timestamp(elementBuffer.getLong()));
                    } else {
                        elementValue = new Value<>(elementBytes);
                    }

                    listValues.add(elementValue);
                    currentOffset += elementSize;
                }

                value = new Value<>(listValues);
            }
            case 3 ->{
                long currentOffset = offset;

                byte[] listSizeBytes = memory.readBytes(currentOffset, Integer.BYTES);
                ByteBuffer listSizeBuffer = ByteBuffer.wrap(listSizeBytes);
                int listSize = listSizeBuffer.getInt();
                currentOffset += Integer.BYTES;

                int perListNullBitmapBytes = (listSize + 7) / 8;
                byte[] perListNullBitmap = memory.readBytes(currentOffset, perListNullBitmapBytes);
                currentOffset += perListNullBitmapBytes;

                List<Value<?>> listValues = new ArrayList<>();
                ColumnMetaData.ColumnDType listDType = getMetaData().getDType();

                for (int j = 0; j < listSize; j++) {
                    boolean isElementNotNull = (perListNullBitmap[j / 8] & (1 << (j % 8))) != 0;

                    if (!isElementNotNull) {
                        listValues.add(new NullValue(new Object()));
                        continue;
                    }

                    byte[] elementSizeBytes = memory.readBytes(currentOffset, Integer.BYTES);
                    ByteBuffer elementSizeBuffer = ByteBuffer.wrap(elementSizeBytes);
                    int elementDataSize = elementSizeBuffer.getInt();
                    currentOffset += Integer.BYTES;

                    byte[] elementBytes = memory.readBytes(currentOffset, elementDataSize);

                    Value<?> elementValue;

                    if (listDType.equals(ColumnMetaData.ColumnDType.LIST_STRING)) {
                        String s = new String(elementBytes);
                        elementValue = new Value<>(s);
                    } else {
                        elementValue = new Value<>(elementBytes);
                    }

                    listValues.add(elementValue);
                    currentOffset += elementDataSize;
                }

                value = new Value<>(listValues);
            }
            default -> value = null;
        }

        return value;
    }

    public Value<?> readRow(int index, DataFrameCursor dataFrameCursor){

        int nullbitmapsize = ((rows + 7) / 8);
        byte[] nullbitmap = memory.readBytes(nullbitmapsize);
        boolean isNull = isNull(index, nullbitmap);
        if(isNull){
            return new NullValue(new Object());
        }
        Value<?> value = null;
        if(dataFrameCursor.getCursor() == 0){
            dataFrameCursor.setCursor(nullbitmapsize);
        }
        switch (columnKind){
            case 0 -> {
                if(getMetaData().getDType().equals(ColumnMetaData.ColumnDType.SCALAR_INT)){
                    byte[] b = memory.readBytes(dataFrameCursor.getCursor(), Integer.BYTES);
                    ByteBuffer buffer = ByteBuffer.wrap(b);
                    Number n = buffer.getInt();
                    value = new Value<>(n);
                    dataFrameCursor.setCursor(dataFrameCursor.getCursor() + Integer.BYTES);
                } else if(getMetaData().getDType().equals(ColumnMetaData.ColumnDType.SCALAR_DOUBLE)){
                    byte[] b = memory.readBytes(dataFrameCursor.getCursor(), Double.BYTES);
                    ByteBuffer buffer = ByteBuffer.wrap(b);
                    Number n = buffer.getDouble();
                    value = new Value<>(n);
                    dataFrameCursor.setCursor(dataFrameCursor.getCursor() + Double.BYTES);
                } else if(getMetaData().getDType().equals(ColumnMetaData.ColumnDType.SCALAR_LOGICAL)){
                    byte[] b = memory.readBytes(dataFrameCursor.getCursor(), 1);
                    ByteBuffer buffer = ByteBuffer.wrap(b);
                    Boolean n = buffer.get() == 1;
                    value = new Value<>(n);
                    dataFrameCursor.setCursor(dataFrameCursor.getCursor() + 1);
                }else if(getMetaData().getDType().equals(ColumnMetaData.ColumnDType.SCALAR_DATE)){
                    byte[] b = memory.readBytes(dataFrameCursor.getCursor(), 8);
                    ByteBuffer buffer = ByteBuffer.wrap(b);
                    long l = buffer.getLong();
                    value = new Value<>(new Date(l));
                    dataFrameCursor.setCursor(dataFrameCursor.getCursor() + 8);
                }else if(getMetaData().getDType().equals(ColumnMetaData.ColumnDType.SCALAR_TIMESTAMP)){
                    byte[] b = memory.readBytes(dataFrameCursor.getCursor(), 8);
                    ByteBuffer buffer = ByteBuffer.wrap(b);
                    long l = buffer.getLong();
                    value = new Value<>(new Timestamp(l));
                    dataFrameCursor.setCursor(dataFrameCursor.getCursor() + 8);
                }
            }
            case 1 -> {
                byte[] sizeBytes = memory.readBytes(dataFrameCursor.getCursor(), Integer.BYTES);
                ByteBuffer sizeBuffer = ByteBuffer.wrap(sizeBytes);
                int dataLength = sizeBuffer.getInt();

                long valueOffset = dataFrameCursor.getCursor() + Integer.BYTES;
                byte[] dataBytes = memory.readBytes(valueOffset, dataLength);

                dataFrameCursor.setCursor(dataFrameCursor.getCursor() + 4);
                dataFrameCursor.setCursor(dataFrameCursor.getCursor() + dataLength);
                if(getMetaData().getDType().equals(ColumnMetaData.ColumnDType.SCALAR_STRING)){
                    String s = new String(dataBytes);
                    value = new Value<>(s);
                } else {
                    value = new Value<>(dataBytes);
                }
            }
            case 2 ->{
                byte[] listSizeBytes = memory.readBytes(dataFrameCursor.getCursor(), Integer.BYTES);
                ByteBuffer listSizeBuffer = ByteBuffer.wrap(listSizeBytes);
                int listSize = listSizeBuffer.getInt();

                long currentOffset = dataFrameCursor.getCursor() + Integer.BYTES;

                int perListNullBitmapBytes = (listSize + 7) / 8;
                byte[] perListNullBitmap = memory.readBytes(currentOffset, perListNullBitmapBytes);
                currentOffset += perListNullBitmapBytes;

                List<Value<?>> listValues = new ArrayList<>();
                int elementSize = getSizePerElement();
                ColumnMetaData.ColumnDType listDType = getMetaData().getDType();

                for (int j = 0; j < listSize; j++) {
                    boolean isElementNotNull = (perListNullBitmap[j / 8] & (1 << (j % 8))) != 0;

                    if (!isElementNotNull) {
                        listValues.add(new NullValue(new Object()));
                        continue;
                    }

                    byte[] elementBytes = memory.readBytes(currentOffset, elementSize);
                    ByteBuffer elementBuffer = ByteBuffer.wrap(elementBytes);
                    Value<?> elementValue;

                    if (listDType.equals(ColumnMetaData.ColumnDType.LIST_INT)) {
                        elementValue = new Value<>(elementBuffer.getInt());
                    } else if (listDType.equals(ColumnMetaData.ColumnDType.LIST_DOUBLE)) {
                        elementValue = new Value<>(elementBuffer.getDouble());
                    } else if (listDType.equals(ColumnMetaData.ColumnDType.LIST_LOGICAL)) {
                        elementValue = new Value<>(elementBuffer.get() == 1);
                    } else if (listDType.equals(ColumnMetaData.ColumnDType.LIST_DATE) || listDType.equals(ColumnMetaData.ColumnDType.LIST_TIMESTAMP)) {
                        elementValue = (listDType.equals(ColumnMetaData.ColumnDType.LIST_DATE)) ?
                                new Value<>(new Date(elementBuffer.getLong())) :
                                new Value<>(new Timestamp(elementBuffer.getLong()));
                    } else {
                        elementValue = new Value<>(elementBytes);
                    }

                    listValues.add(elementValue);
                    currentOffset += elementSize;
                }
                dataFrameCursor.setCursor(currentOffset);
                value = new Value<>(listValues);
            }
            case 3 ->{
                long currentOffset = dataFrameCursor.getCursor();

                byte[] listSizeBytes = memory.readBytes(currentOffset, Integer.BYTES);
                ByteBuffer listSizeBuffer = ByteBuffer.wrap(listSizeBytes);
                int listSize = listSizeBuffer.getInt();
                currentOffset += Integer.BYTES;

                int perListNullBitmapBytes = (listSize + 7) / 8;
                byte[] perListNullBitmap = memory.readBytes(currentOffset, perListNullBitmapBytes);
                currentOffset += perListNullBitmapBytes;

                List<Value<?>> listValues = new ArrayList<>();
                ColumnMetaData.ColumnDType listDType = getMetaData().getDType();

                for (int j = 0; j < listSize; j++) {
                    boolean isElementNotNull = (perListNullBitmap[j / 8] & (1 << (j % 8))) != 0;

                    if (!isElementNotNull) {
                        listValues.add(new NullValue(new Object()));
                        continue;
                    }

                    byte[] elementSizeBytes = memory.readBytes(currentOffset, Integer.BYTES);
                    ByteBuffer elementSizeBuffer = ByteBuffer.wrap(elementSizeBytes);
                    int elementDataSize = elementSizeBuffer.getInt();
                    currentOffset += Integer.BYTES;

                    byte[] elementBytes = memory.readBytes(currentOffset, elementDataSize);

                    Value<?> elementValue;

                    if (listDType.equals(ColumnMetaData.ColumnDType.LIST_STRING)) {
                        String s = new String(elementBytes);
                        elementValue = new Value<>(s);
                    } else {
                        elementValue = new Value<>(elementBytes);
                    }

                    listValues.add(elementValue);
                    currentOffset += elementDataSize;
                }
                dataFrameCursor.setCursor(currentOffset);
                value = new Value<>(listValues);
            }
            default -> value = null;
        }

        return value;
    }

    public boolean isNull(int index) {
        int nullbitmapsize = ((rows + 7) / 8);
        byte[] nullbitmap = memory.readBytes(nullbitmapsize);
        boolean isNotNull = (nullbitmap[index / 8] & (1 << (index % 8))) != 0;
        return !isNotNull;
    }

    private boolean isNull(int index, byte[] nullbitmap){
        boolean isNotNull = (nullbitmap[index / 8] & (1 << (index % 8))) != 0;
        return !isNotNull;
    }

    public Column distributeColumn(List<Integer> indexList) throws KException {
        int newRowCount = indexList.size();

        boolean[] newNotNullFlags = new boolean[newRowCount];

        long srcBitmapSize = (this.rows + 7) / 8;
        byte[] srcBitmap = memory.readBytes((int) srcBitmapSize);

        for (int i = 0; i < newRowCount; i++) {
            int srcIdx = indexList.get(i);
            boolean isNotNull = (srcBitmap[srcIdx / 8] & (1 << (srcIdx % 8))) != 0;
            newNotNullFlags[i] = isNotNull;
        }

        long[] rowDataOffsets = new long[this.rows];

        if (columnKind != 0) {
            long cursor = srcBitmapSize;

            for (int i = 0; i < this.rows; i++) {
                boolean isNotNull = (srcBitmap[i / 8] & (1 << (i % 8))) != 0;
                rowDataOffsets[i] = cursor;

                if (isNotNull) {
                    if (columnKind == 1) {
                        byte[] sizeBytes = memory.readBytes(cursor, Integer.BYTES);
                        int size = ByteBuffer.wrap(sizeBytes).getInt();
                        cursor += Integer.BYTES + size;
                    } else if (columnKind == 2) {
                        byte[] listSizeBytes = memory.readBytes(cursor, Integer.BYTES);
                        int listSize = ByteBuffer.wrap(listSizeBytes).getInt();
                        int bitmapBytes = (listSize + 7) / 8;
                        cursor += Integer.BYTES + bitmapBytes + ((long) listSize * sizePerElement);
                    } else if (columnKind == 3) {
                        byte[] listSizeBytes = memory.readBytes(cursor, Integer.BYTES);
                        int listSize = ByteBuffer.wrap(listSizeBytes).getInt();
                        cursor += Integer.BYTES;

                        int bitmapBytes = (listSize + 7) / 8;
                        byte[] listBitmap = memory.readBytes(cursor, bitmapBytes);
                        cursor += bitmapBytes;
                        for (int j = 0; j < listSize; j++) {
                            boolean isElNotNull = (listBitmap[j / 8] & (1 << (j % 8))) != 0;
                            if (isElNotNull) {
                                byte[] elSizeBytes = memory.readBytes(cursor, Integer.BYTES);
                                int elSize = ByteBuffer.wrap(elSizeBytes).getInt();
                                cursor += Integer.BYTES + elSize;
                            }
                        }
                    }
                }
            }
        }

        Column distributedColumn = null;
        ColumnMetaData.ColumnDType dType = metaData.getDType();

        switch (columnKind) {
            case 0: {
                int totalSize = newRowCount * sizePerElement;
                ByteBuffer buffer = ByteBuffer.allocate(totalSize);

                for (int i = 0; i < newRowCount; i++) {
                    int srcIdx = indexList.get(i);
                    long offset = srcBitmapSize + ((long) (this.startIdx + srcIdx) * sizePerElement);
                    byte[] data = memory.readBytes(offset, sizePerElement);
                    buffer.put(data);
                }
                buffer.flip();

                distributedColumn = new Column(metaData.getName(), sizePerElement, memoryGroupName, buffer, newNotNullFlags, sizePerElement, 0, newRowCount, dType);
                break;
            }
            case 1: {
                List<VariableElement> values = new ArrayList<>();
                for (int i = 0; i < newRowCount; i++) {
                    if (newNotNullFlags[i]) {
                        int srcIdx = indexList.get(i);
                        long offset = rowDataOffsets[srcIdx];

                        byte[] sizeBytes = memory.readBytes(offset, Integer.BYTES);
                        int size = ByteBuffer.wrap(sizeBytes).getInt();
                        byte[] data = memory.readBytes(offset + Integer.BYTES, size);

                        values.add(new VariableElement(data));
                    }
                }
                distributedColumn = new Column(metaData.getName(), values, memoryGroupName, newNotNullFlags, 0, newRowCount, dType);
                break;
            }
            case 2: {
                List<List<byte[]>> lists = new ArrayList<>();
                List<boolean[]> perListNotNullFlags = new ArrayList<>();

                for (int i = 0; i < newRowCount; i++) {
                    if (newNotNullFlags[i]) {
                        int srcIdx = indexList.get(i);
                        long offset = rowDataOffsets[srcIdx];

                        byte[] listSizeBytes = memory.readBytes(offset, Integer.BYTES);
                        int listSize = ByteBuffer.wrap(listSizeBytes).getInt();
                        offset += Integer.BYTES;

                        int bitmapBytes = (listSize + 7) / 8;
                        byte[] listBitmapRaw = memory.readBytes(offset, bitmapBytes);
                        offset += bitmapBytes;

                        boolean[] listFlags = new boolean[listSize];
                        for (int bit = 0; bit < listSize; bit++) {
                            listFlags[bit] = (listBitmapRaw[bit / 8] & (1 << (bit % 8))) != 0;
                        }
                        perListNotNullFlags.add(listFlags);

                        List<byte[]> elementList = new ArrayList<>();
                        for (int j = 0; j < listSize; j++) {
                            if (listFlags[j]) {
                                byte[] el = memory.readBytes(offset, sizePerElement);
                                elementList.add(el);
                            }
                            offset += sizePerElement;
                        }
                        lists.add(elementList);
                    }
                }
                distributedColumn = new Column(metaData.getName(), memoryGroupName, lists, perListNotNullFlags, newNotNullFlags, sizePerElement, 0, newRowCount, dType);
                break;
            }
            case 3: {
                List<List<VariableElement>> lists = new ArrayList<>();
                List<boolean[]> perListNotNullFlags = new ArrayList<>();

                for (int i = 0; i < newRowCount; i++) {
                    if (newNotNullFlags[i]) {
                        int srcIdx = indexList.get(i);
                        long offset = rowDataOffsets[srcIdx];

                        byte[] listSizeBytes = memory.readBytes(offset, Integer.BYTES);
                        int listSize = ByteBuffer.wrap(listSizeBytes).getInt();
                        offset += Integer.BYTES;

                        int bitmapBytes = (listSize + 7) / 8;
                        byte[] listBitmapRaw = memory.readBytes(offset, bitmapBytes);
                        offset += bitmapBytes;

                        boolean[] listFlags = new boolean[listSize];
                        for (int bit = 0; bit < listSize; bit++) {
                            listFlags[bit] = (listBitmapRaw[bit / 8] & (1 << (bit % 8))) != 0;
                        }
                        perListNotNullFlags.add(listFlags);

                        List<VariableElement> elementList = new ArrayList<>();
                        for (int j = 0; j < listSize; j++) {
                            if (listFlags[j]) {
                                byte[] elSizeBytes = memory.readBytes(offset, Integer.BYTES);
                                int elSize = ByteBuffer.wrap(elSizeBytes).getInt();
                                offset += Integer.BYTES;

                                byte[] elData = memory.readBytes(offset, elSize);
                                offset += elSize;
                                elementList.add(new VariableElement(elData));
                            }
                        }
                        lists.add(elementList);
                    }
                }
                distributedColumn = new Column(metaData.getName(), memoryGroupName, lists, perListNotNullFlags, newNotNullFlags, 0, newRowCount, dType);
                break;
            }
        }

        if (distributedColumn != null) {
            distributedColumn.id = this.id;
            distributedColumn.metaData.setRows(newRowCount);
            distributedColumn.metaData.setSharded(true);
        }

        return distributedColumn;
    }


    @Override
    public String toString() {
        DataFrameCursor cursor = new DataFrameCursor();

        Value<?> first = readRow(0, cursor);
        return first.toString();

    }
}
