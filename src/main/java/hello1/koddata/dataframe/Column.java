package hello1.koddata.dataframe;

import hello1.koddata.concurrent.cluster.ClusterIdCounter;
import hello1.koddata.engine.NullValue;
import hello1.koddata.engine.Value;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.memory.Memory;
import hello1.koddata.memory.MemoryGroup;
import hello1.koddata.net.NodeStatus;
import hello1.koddata.utils.KodResourceNaming;
import hello1.koddata.utils.Serializable;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

public class Column implements Serializable, KodResourceNaming {
    private static ClusterIdCounter columnIdCounter;
    private long id;
    private String memoryGroupName;
    private ColumnMetaData metaData;
    private Memory memory;
    private byte columnKind;
    private int sizePerElement;
    private int startIdx;
    private int endIdx;
    private int rows;
    //for deserializing
    public Column(){}

    public static void setupIdCounter(Set<NodeStatus> peers){
        columnIdCounter = ClusterIdCounter.getCounter(Column.class, peers);
    }

    public Column(String name, int sizePerElement, String memoryGroupName, ByteBuffer dataBuffer, boolean[] notNullFlags, int elementSize, int startIdx, int endIdx, ColumnMetaData.ColumnDType dType) throws KException {
        setupMetadata(name, sizePerElement, memoryGroupName, false, dType);
        id = columnIdCounter.count();
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
        id = columnIdCounter.count();

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
        id = columnIdCounter.count();

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
        id = columnIdCounter.count();
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

    @Override
    public String getResourceName() {
        return String.format("%s::%d::%s", Column.class.getName(), id, getMetaData().getName());
    }

    @Override
    public byte[] serialize() {
        // 1. คำนวณขนาดของ Buffer
        int metaDataSize = 0;
        if (metaData != null) {

            // **ส่วนที่ 1: ขนาด DType**
            // 4 (int length) + DType name length
            String dTypeName = (metaData.getDType() != null) ? metaData.getDType().name() : "";
            int dTypeSize = 4 + dTypeName.length();

            metaDataSize =
                    4 + metaData.getName().length() + // name length + data
                            1 + // isVariable (boolean)
                            Integer.BYTES + // rows
                            1 + // isSharded (boolean)
                            dTypeSize; // **เพิ่ม DType size**
        }

        long memoryDataSize = 0;
        if(memory != null){
            memoryDataSize = memory.size();
        }

        int bufferSize = Long.BYTES + // id
                4 + memoryGroupName.length() + // memoryGroupName length + data
                metaDataSize +
                Byte.BYTES + // columnKind
                Integer.BYTES + // sizePerElement
                Integer.BYTES + // startIdx
                Integer.BYTES + // endIdx
                Long.BYTES + // allocatedSize ของ Memory
                (int) memoryDataSize; // ข้อมูลจริงของ Memory

        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);

        // 2. Serialize ข้อมูล Column หลัก
        buffer.putLong(id);

        // memoryGroupName
        byte[] memoryGroupNameBytes = memoryGroupName.getBytes();
        buffer.putInt(memoryGroupNameBytes.length);
        buffer.put(memoryGroupNameBytes);

        // 3. Serialize ColumnMetaData
        if (metaData != null) {
            // name
            byte[] metaNameBytes = metaData.getName().getBytes();
            buffer.putInt(metaNameBytes.length);
            buffer.put(metaNameBytes);

            // isVariable
            buffer.put(metaData.isVariable() ? (byte) 1 : (byte) 0);

            // rows
            buffer.putInt(metaData.getRows());

            // isSharded
            buffer.put(metaData.isSharded() ? (byte) 1 : (byte) 0);

            // **ส่วนที่ 2: Serialize dType**
            String dTypeName = (metaData.getDType() != null) ? metaData.getDType().name() : "";
            byte[] dTypeNameBytes = dTypeName.getBytes();
            buffer.putInt(dTypeNameBytes.length);
            buffer.put(dTypeNameBytes);
        }

        // 4. Serialize ข้อมูล Column ที่เหลือ
        buffer.put(columnKind);
        buffer.putInt(sizePerElement);
        buffer.putInt(startIdx);
        buffer.putInt(endIdx);

        // 5. Serialize Memory Data
        if (memory != null) {
            buffer.putLong(memory.size());
            byte[] rawData = memory.readBytes((int) memory.size());
            buffer.put(rawData);
        } else {
            buffer.putLong(0L);
        }

        return buffer.array();
    }

    @Override
    public void deserialize(byte[] b) {
        ByteBuffer buffer = ByteBuffer.wrap(b);

        // 1. Deserialize ข้อมูล Column หลัก
        this.id = buffer.getLong();

        // memoryGroupName
        int nameLength = buffer.getInt();
        byte[] nameBytes = new byte[nameLength];
        buffer.get(nameBytes);
        this.memoryGroupName = new String(nameBytes);

        // 2. Deserialize ColumnMetaData

        // name
        int metaNameLength = buffer.getInt();
        byte[] metaNameBytes = new byte[metaNameLength];
        buffer.get(metaNameBytes);
        String metaName = new String(metaNameBytes);

        // isVariable
        boolean isVariable = (buffer.get() == 1);

        // rows
        int rows = buffer.getInt();

        // isSharded
        boolean isSharded = (buffer.get() == 1);

        // **ส่วนที่ 1: Deserialize dType**
        int dTypeNameLength = buffer.getInt();
        byte[] dTypeNameBytes = new byte[dTypeNameLength];
        buffer.get(dTypeNameBytes);
        String dTypeName = new String(dTypeNameBytes);

        ColumnMetaData.ColumnDType dType = null;
        if (!dTypeName.isEmpty()) {
            try {
                // แปลงชื่อ Enum กลับไปเป็น Object
                dType = ColumnMetaData.ColumnDType.valueOf(dTypeName);
            } catch (IllegalArgumentException e) {
                // ควรจัดการข้อผิดพลาด (เช่น ถ้ามีการเปลี่ยนแปลง Enum ระหว่างเวอร์ชัน)
            }
        }

        // **ส่วนที่ 2: สร้าง ColumnMetaData โดยใช้ Constructor ใหม่**
        // สมมติว่า ColumnMetaData มี Constructor (String name, boolean isVariable, ColumnDType dType)
        this.metaData = new ColumnMetaData(metaName, isVariable, dType);

        // ตั้งค่า rows โดยใช้ Setter
        metaData.setRows(rows);

        // ตั้งค่า isSharded โดยใช้ Setter
        metaData.setSharded(isSharded);

        // 3. Deserialize ข้อมูล Column ที่เหลือ
        this.columnKind = buffer.get();
        this.sizePerElement = buffer.getInt();
        this.startIdx = buffer.getInt();
        this.endIdx = buffer.getInt();

        // 4. Deserialize และกู้คืน Memory Data
        long allocatedSize = buffer.getLong();

        if (allocatedSize > 0) {
            byte[] rawData = new byte[(int) allocatedSize];
            buffer.get(rawData);

            // **ใช้ WritableMemory/Unsafe เพื่อกู้คืน Memory object**
            // (อ้างอิงจากสมมติฐานในคำตอบก่อนหน้าว่าระบบมีกลไกในการสร้าง Memory จาก rawData)
            // this.memory = WritableMemory.createFromRawData(rawData); // สมมติฐาน
        } else {
            this.memory = null;
        }
    }

    public Column distributeColumn(int startIdx, int endIdx) throws KException {
        int newRowCount = endIdx - startIdx;

        // 1. สร้าง Null Flags สำหรับ Slice ใหม่
        boolean[] newNotNullFlags = new boolean[newRowCount];
        for (int i = 0; i < newRowCount; i++) {
            newNotNullFlags[i] = !isNull(this.startIdx + startIdx + i);
        }

        Column distributedColumn = null;
        ColumnMetaData.ColumnDType dType = metaData.getDType();

        // 2. ดึงข้อมูลและสร้าง Column ใหม่ตามประเภท (ColumnKind)
        switch (columnKind) {
            case 0: { // Fixed Scalar
                long nullBitmapSize = (rows + 7) / 8;
                long dataStartOffset = nullBitmapSize + ((long) (this.startIdx + startIdx) * sizePerElement);
                int dataLength = newRowCount * sizePerElement;

                byte[] slicedData = memory.readBytes(dataStartOffset, dataLength);
                ByteBuffer dataBuffer = ByteBuffer.wrap(slicedData);

                distributedColumn = new Column(metaData.getName(), sizePerElement, memoryGroupName, dataBuffer, newNotNullFlags, sizePerElement, 0, newRowCount, dType);
                break;
            }
            case 1: { // Variable Scalar (Strict Compact/Dense Logic)
                List<VariableElement> values = new ArrayList<>();

                // 2.1 Scan เพื่อหาจุดเริ่มต้น (Skip ข้อมูลก่อนหน้า startIdx จริง)
                long cursor = (rows + 7) / 8;
                for (int i = 0; i < this.startIdx + startIdx; i++) {
                    if (!isNull(i)) {
                        byte[] sizeBytes = memory.readBytes(cursor, Integer.BYTES);
                        int size = ByteBuffer.wrap(sizeBytes).getInt();
                        cursor += Integer.BYTES + size;
                    }
                }

                // 2.2 อ่านข้อมูลจริงในช่วง Slice
                for (int i = 0; i < newRowCount; i++) {
                    if (newNotNullFlags[i]) { // ถ้า Row ไม่เป็น Null
                        byte[] sizeBytes = memory.readBytes(cursor, Integer.BYTES);
                        int size = ByteBuffer.wrap(sizeBytes).getInt();
                        cursor += Integer.BYTES;

                        byte[] data = memory.readBytes(cursor, size);
                        cursor += size;

                        values.add(new VariableElement(data)); // เพิ่มเฉพาะ Non-Null
                    }
                    // ถ้าเป็น Null: ข้ามการเพิ่มใน values
                }

                distributedColumn = new Column(metaData.getName(), values, memoryGroupName, newNotNullFlags, 0, newRowCount, dType);
                break;
            }
            case 2: { // Fixed List (Strict Compact/Dense Logic for Row and Element)
                List<List<byte[]>> lists = new ArrayList<>();
                List<boolean[]> perListNotNullFlags = new ArrayList<>();

                // 2.1 Scan เพื่อหาจุดเริ่มต้น (Compact Logic)
                long cursor = (rows + 7) / 8;
                for(int i=0; i < this.startIdx + startIdx; i++){
                    if(!isNull(i)){
                        byte[] listSizeBytes = memory.readBytes(cursor, Integer.BYTES);
                        int listSize = ByteBuffer.wrap(listSizeBytes).getInt();
                        cursor += Integer.BYTES;

                        int bitmapBytes = (listSize + 7) / 8;
                        cursor += bitmapBytes;

                        cursor += (long) listSize * sizePerElement;
                    }
                }

                // 2.2 อ่านข้อมูล Slice
                for(int i=0; i < newRowCount; i++){
                    if(newNotNullFlags[i]){ // ถ้า Row ไม่เป็น Null
                        byte[] listSizeBytes = memory.readBytes(cursor, Integer.BYTES);
                        int listSize = ByteBuffer.wrap(listSizeBytes).getInt();
                        cursor += Integer.BYTES;

                        int bitmapBytes = (listSize + 7) / 8;
                        byte[] listBitmapRaw = memory.readBytes(cursor, bitmapBytes);
                        cursor += bitmapBytes;

                        boolean[] listFlags = new boolean[listSize];
                        for(int bit=0; bit<listSize; bit++){
                            listFlags[bit] = (listBitmapRaw[bit/8] & (1 << (bit%8))) != 0;
                        }
                        perListNotNullFlags.add(listFlags); // เพิ่ม listFlags เดิม

                        // *** การเปลี่ยนแปลง: elementList เป็น Dense List ***
                        List<byte[]> elementList = new ArrayList<>();
                        for(int j=0; j<listSize; j++){
                            if(listFlags[j]){ // ถ้า Element ไม่เป็น Null
                                byte[] el = memory.readBytes(cursor, sizePerElement);
                                elementList.add(el); // เพิ่มเฉพาะ Non-Null Element
                            }
                            cursor += sizePerElement;
                            // ถ้าเป็น Null: ข้ามการเพิ่มใน elementList
                        }
                        lists.add(elementList); // เพิ่มเฉพาะ Non-Null Row
                    }
                    // ถ้าเป็น Null: ข้ามการเพิ่มใน lists และ perListNotNullFlags
                }

                distributedColumn = new Column(metaData.getName(), memoryGroupName, lists, perListNotNullFlags, newNotNullFlags, sizePerElement, 0, newRowCount, dType);
                break;
            }
            case 3: { // Variable List (Strict Compact/Dense Logic for Row and Element)
                List<List<VariableElement>> lists = new ArrayList<>();
                List<boolean[]> perListNotNullFlags = new ArrayList<>();

                // 2.1 Scan เพื่อหาจุดเริ่มต้น (Compact Logic)
                long cursor = (rows + 7) / 8;
                for(int i=0; i < this.startIdx + startIdx; i++){
                    if(!isNull(i)){
                        byte[] listSizeBytes = memory.readBytes(cursor, Integer.BYTES);
                        int listSize = ByteBuffer.wrap(listSizeBytes).getInt();
                        cursor += Integer.BYTES;

                        int bitmapBytes = (listSize + 7) / 8;
                        byte[] listBitmap = memory.readBytes(cursor, bitmapBytes);
                        cursor += bitmapBytes;

                        // Skip elements inside list
                        for(int j=0; j<listSize; j++){
                            boolean isElNotNull = (listBitmap[j/8] & (1 << (j%8))) != 0;
                            if(isElNotNull){
                                byte[] elSizeBytes = memory.readBytes(cursor, Integer.BYTES);
                                int elSize = ByteBuffer.wrap(elSizeBytes).getInt();
                                cursor += Integer.BYTES + elSize;
                            }
                        }
                    }
                }

                // 2.2 อ่านข้อมูล Slice
                for(int i = 0; i < newRowCount; i++){
                    if(newNotNullFlags[i]){ // ถ้า Row ไม่เป็น Null
                        byte[] listSizeBytes = memory.readBytes(cursor, Integer.BYTES);
                        int listSize = ByteBuffer.wrap(listSizeBytes).getInt();
                        cursor += Integer.BYTES;

                        int bitmapBytes = (listSize + 7) / 8;
                        byte[] listBitmapRaw = memory.readBytes(cursor, bitmapBytes);
                        cursor += bitmapBytes;

                        boolean[] listFlags = new boolean[listSize];
                        for(int bit = 0; bit < listSize; bit++){
                            listFlags[bit] = (listBitmapRaw[bit/8] & (1 << (bit%8))) != 0;
                        }
                        perListNotNullFlags.add(listFlags); // เพิ่ม listFlags เดิม

                        // *** การเปลี่ยนแปลง: elementList เป็น Dense List ***
                        List<VariableElement> elementList = new ArrayList<>();
                        for(int j = 0; j < listSize; j++){
                            if(listFlags[j]){ // ถ้า Element ไม่เป็น Null
                                byte[] elSizeBytes = memory.readBytes(cursor, Integer.BYTES);
                                int elSize = ByteBuffer.wrap(elSizeBytes).getInt();
                                cursor += Integer.BYTES;

                                byte[] elData = memory.readBytes(cursor, elSize);
                                cursor += elSize;
                                elementList.add(new VariableElement(elData)); // เพิ่มเฉพาะ Non-Null Element
                            }
                            // ถ้าเป็น Null: ข้ามการเพิ่มใน elementList
                        }
                        lists.add(elementList); // เพิ่มเฉพาะ Non-Null Row
                    }
                    // ถ้าเป็น Null: ข้ามการเพิ่มใน lists และ perListNotNullFlags
                }

                distributedColumn = new Column(metaData.getName(), memoryGroupName, lists, perListNotNullFlags, newNotNullFlags, 0, newRowCount, dType);
                break;
            }
        }

        // 3. Resource Identity และ Metadata View
        if (distributedColumn != null) {
            distributedColumn.id = this.id;

            distributedColumn.startIdx = this.startIdx + startIdx;
            distributedColumn.endIdx = this.startIdx + endIdx;

            distributedColumn.metaData.setRows(newRowCount);
            distributedColumn.metaData.setSharded(true);
        }

        return distributedColumn;
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
//                แบบ variable
//                byte[] size = memory.readBytes(offset, 4);
                // 1. อ่านขนาดของ Element (4 bytes) จากตำแหน่ง dataOffset
                byte[] sizeBytes = memory.readBytes(offset, Integer.BYTES);
                ByteBuffer sizeBuffer = ByteBuffer.wrap(sizeBytes);
                int dataLength = sizeBuffer.getInt();

                // 2. ข้าม 4 bytes ของขนาด, อ่านข้อมูลจริง (dataLength bytes)
                long valueOffset = offset + Integer.BYTES;
                byte[] dataBytes = memory.readBytes(valueOffset, dataLength);

                // 3. แปลง byte array เป็น Value<?>
                // โค้ดต้นฉบับไม่ได้ระบุประเภทข้อมูล (DType) สำหรับ Variable Length อย่างชัดเจน
                // (เช่น String, byte[]) ผมจะสมมติว่าข้อมูลที่อ่านได้คือ byte[] หรือ String (แล้วแต่การใช้งานใน VariableElement)
                // แต่เนื่องจาก DType ไม่ได้ถูกใช้ในการแปลง (เหมือนใน case 0) ผมจึงแปลงเป็น byte[] เพื่อให้ใช้งานได้:

                if(getMetaData().getDType().equals(ColumnMetaData.ColumnDType.SCALAR_STRING)){
                    // สมมติเป็น String
                    String s = new String(dataBytes);
                    value = new Value<>(s);
                } else {
                    // ใช้ byte[] เป็นค่าเริ่มต้นสำหรับข้อมูลแบบ Variable length
                    value = new Value<>(dataBytes);
                }
            }
            case 2 ->{
//                แบบ list of fix length
                // 1. อ่านขนาดของ List (4 bytes)
                byte[] listSizeBytes = memory.readBytes(offset, Integer.BYTES);
                ByteBuffer listSizeBuffer = ByteBuffer.wrap(listSizeBytes);
                int listSize = listSizeBuffer.getInt();

                long currentOffset = offset + Integer.BYTES; // ข้าม List Size

                // 2. คำนวณและอ่าน Per-List Null Bitmap
                int perListNullBitmapBytes = (listSize + 7) / 8;
                byte[] perListNullBitmap = memory.readBytes(currentOffset, perListNullBitmapBytes);
                currentOffset += perListNullBitmapBytes;

                // 3. อ่าน Element ข้อมูลจริง (List)
                List<Value<?>> listValues = new ArrayList<>();
                int elementSize = getSizePerElement();

                // ดึง DType ของ Column เพื่อใช้ในการแปลงค่า Element ภายใน
                ColumnMetaData.ColumnDType listDType = getMetaData().getDType();

                for (int j = 0; j < listSize; j++) {
                    boolean isElementNotNull = (perListNullBitmap[j / 8] & (1 << (j % 8))) != 0;

                    if (!isElementNotNull) {
                        listValues.add(new NullValue(new Object())); // Element เป็น Null
                        continue;
                    }

                    // อ่าน Element ที่มีขนาดคงที่
                    byte[] elementBytes = memory.readBytes(currentOffset, elementSize);
                    ByteBuffer elementBuffer = ByteBuffer.wrap(elementBytes);
                    Value<?> elementValue;

                    // ตรรกะการแปลงค่า DType ภายใน List (List DType -> Element Type)
                    if (listDType.equals(ColumnMetaData.ColumnDType.LIST_INT)) {
                        elementValue = new Value<>(elementBuffer.getInt());
                    } else if (listDType.equals(ColumnMetaData.ColumnDType.LIST_DOUBLE)) {
                        elementValue = new Value<>(elementBuffer.getDouble());
                    } else if (listDType.equals(ColumnMetaData.ColumnDType.LIST_LOGICAL)) {
                        elementValue = new Value<>(elementBuffer.get() == 1);
                    } else if (listDType.equals(ColumnMetaData.ColumnDType.LIST_DATE) || listDType.equals(ColumnMetaData.ColumnDType.LIST_TIMESTAMP)) {
                        // Date/Timestamp ใช้ Long
                        elementValue = (listDType.equals(ColumnMetaData.ColumnDType.LIST_DATE)) ?
                                new Value<>(new Date(elementBuffer.getLong())) :
                                new Value<>(new Timestamp(elementBuffer.getLong()));
                    } else {
                        // สำหรับ DType อื่นๆ หรือไม่รู้จัก ให้ใช้ byte[] เป็นค่าเริ่มต้น
                        elementValue = new Value<>(elementBytes);
                    }

                    listValues.add(elementValue);
                    currentOffset += elementSize; // เลื่อน Offset
                }

                value = new Value<>(listValues); // คืนค่าเป็น Value ที่บรรจุ List<Value<?>>
            }
            case 3 ->{
//                แบบ list variable length
                long currentOffset = offset;

                // 1. อ่าน List Size (4 bytes)
                byte[] listSizeBytes = memory.readBytes(currentOffset, Integer.BYTES);
                ByteBuffer listSizeBuffer = ByteBuffer.wrap(listSizeBytes);
                int listSize = listSizeBuffer.getInt();
                currentOffset += Integer.BYTES; // ข้าม List Size

                // 2. คำนวณและอ่าน Per-List Null Bitmap
                int perListNullBitmapBytes = (listSize + 7) / 8;
                byte[] perListNullBitmap = memory.readBytes(currentOffset, perListNullBitmapBytes);
                currentOffset += perListNullBitmapBytes;

                // 3. อ่าน Element ข้อมูลจริง (List)
                List<Value<?>> listValues = new ArrayList<>();
                ColumnMetaData.ColumnDType listDType = getMetaData().getDType();

                for (int j = 0; j < listSize; j++) {
                    boolean isElementNotNull = (perListNullBitmap[j / 8] & (1 << (j % 8))) != 0;

                    if (!isElementNotNull) {
                        listValues.add(new NullValue(new Object())); // Element เป็น Null
                        continue;
                    }

                    // Element เป็นแบบ Variable Length: [Size (4 bytes)][Data (N bytes)]

                    // 3a. อ่านขนาดของ Element (4 bytes)
                    byte[] elementSizeBytes = memory.readBytes(currentOffset, Integer.BYTES);
                    ByteBuffer elementSizeBuffer = ByteBuffer.wrap(elementSizeBytes);
                    int elementDataSize = elementSizeBuffer.getInt();
                    currentOffset += Integer.BYTES;

                    // 3b. อ่านข้อมูลจริง
                    byte[] elementBytes = memory.readBytes(currentOffset, elementDataSize);

                    // 3c. แปลงค่า DType
                    Value<?> elementValue;

                    // Note: สำหรับ Variable Length DType ที่สำคัญคือ String (LIST_STRING)
                    if (listDType.equals(ColumnMetaData.ColumnDType.LIST_STRING)) {
                        String s = new String(elementBytes);
                        elementValue = new Value<>(s);
                    } else {
                        // สำหรับ DType อื่นๆ หรือไม่รู้จัก ให้ใช้ byte[]
                        elementValue = new Value<>(elementBytes);
                    }

                    listValues.add(elementValue);
                    currentOffset += elementDataSize; // เลื่อน Offset ข้ามข้อมูลจริง
                }

                value = new Value<>(listValues); // คืนค่าเป็น Value ที่บรรจุ List<Value<?>>
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
//                แบบ variable
//                byte[] size = memory.readBytes(offset, 4);
                // 1. อ่านขนาดของ Element (4 bytes) จากตำแหน่ง dataOffset
                byte[] sizeBytes = memory.readBytes(dataFrameCursor.getCursor(), Integer.BYTES);
                ByteBuffer sizeBuffer = ByteBuffer.wrap(sizeBytes);
                int dataLength = sizeBuffer.getInt();

                // 2. ข้าม 4 bytes ของขนาด, อ่านข้อมูลจริง (dataLength bytes)
                long valueOffset = dataFrameCursor.getCursor() + Integer.BYTES;
                byte[] dataBytes = memory.readBytes(valueOffset, dataLength);

                // 3. แปลง byte array เป็น Value<?>
                // โค้ดต้นฉบับไม่ได้ระบุประเภทข้อมูล (DType) สำหรับ Variable Length อย่างชัดเจน
                // (เช่น String, byte[]) ผมจะสมมติว่าข้อมูลที่อ่านได้คือ byte[] หรือ String (แล้วแต่การใช้งานใน VariableElement)
                // แต่เนื่องจาก DType ไม่ได้ถูกใช้ในการแปลง (เหมือนใน case 0) ผมจึงแปลงเป็น byte[] เพื่อให้ใช้งานได้:
                dataFrameCursor.setCursor(dataFrameCursor.getCursor() + 4);
                dataFrameCursor.setCursor(dataFrameCursor.getCursor() + dataLength);
                if(getMetaData().getDType().equals(ColumnMetaData.ColumnDType.SCALAR_STRING)){
                    // สมมติเป็น String
                    String s = new String(dataBytes);
                    value = new Value<>(s);
                } else {
                    // ใช้ byte[] เป็นค่าเริ่มต้นสำหรับข้อมูลแบบ Variable length
                    value = new Value<>(dataBytes);
                }
            }
            case 2 ->{
//                แบบ list of fix length
                // 1. อ่านขนาดของ List (4 bytes)
                byte[] listSizeBytes = memory.readBytes(dataFrameCursor.getCursor(), Integer.BYTES);
                ByteBuffer listSizeBuffer = ByteBuffer.wrap(listSizeBytes);
                int listSize = listSizeBuffer.getInt();

                long currentOffset = dataFrameCursor.getCursor() + Integer.BYTES; // ข้าม List Size

                // 2. คำนวณและอ่าน Per-List Null Bitmap
                int perListNullBitmapBytes = (listSize + 7) / 8;
                byte[] perListNullBitmap = memory.readBytes(currentOffset, perListNullBitmapBytes);
                currentOffset += perListNullBitmapBytes;

                // 3. อ่าน Element ข้อมูลจริง (List)
                List<Value<?>> listValues = new ArrayList<>();
                int elementSize = getSizePerElement();

                // ดึง DType ของ Column เพื่อใช้ในการแปลงค่า Element ภายใน
                ColumnMetaData.ColumnDType listDType = getMetaData().getDType();

                for (int j = 0; j < listSize; j++) {
                    boolean isElementNotNull = (perListNullBitmap[j / 8] & (1 << (j % 8))) != 0;

                    if (!isElementNotNull) {
                        listValues.add(new NullValue(new Object())); // Element เป็น Null
                        continue;
                    }

                    // อ่าน Element ที่มีขนาดคงที่
                    byte[] elementBytes = memory.readBytes(currentOffset, elementSize);
                    ByteBuffer elementBuffer = ByteBuffer.wrap(elementBytes);
                    Value<?> elementValue;

                    // ตรรกะการแปลงค่า DType ภายใน List (List DType -> Element Type)
                    if (listDType.equals(ColumnMetaData.ColumnDType.LIST_INT)) {
                        elementValue = new Value<>(elementBuffer.getInt());
                    } else if (listDType.equals(ColumnMetaData.ColumnDType.LIST_DOUBLE)) {
                        elementValue = new Value<>(elementBuffer.getDouble());
                    } else if (listDType.equals(ColumnMetaData.ColumnDType.LIST_LOGICAL)) {
                        elementValue = new Value<>(elementBuffer.get() == 1);
                    } else if (listDType.equals(ColumnMetaData.ColumnDType.LIST_DATE) || listDType.equals(ColumnMetaData.ColumnDType.LIST_TIMESTAMP)) {
                        // Date/Timestamp ใช้ Long
                        elementValue = (listDType.equals(ColumnMetaData.ColumnDType.LIST_DATE)) ?
                                new Value<>(new Date(elementBuffer.getLong())) :
                                new Value<>(new Timestamp(elementBuffer.getLong()));
                    } else {
                        // สำหรับ DType อื่นๆ หรือไม่รู้จัก ให้ใช้ byte[] เป็นค่าเริ่มต้น
                        elementValue = new Value<>(elementBytes);
                    }

                    listValues.add(elementValue);
                    currentOffset += elementSize; // เลื่อน Offset
                }
                dataFrameCursor.setCursor(currentOffset);
                value = new Value<>(listValues); // คืนค่าเป็น Value ที่บรรจุ List<Value<?>>
            }
            case 3 ->{
//                แบบ list variable length
                long currentOffset = dataFrameCursor.getCursor();

                // 1. อ่าน List Size (4 bytes)
                byte[] listSizeBytes = memory.readBytes(currentOffset, Integer.BYTES);
                ByteBuffer listSizeBuffer = ByteBuffer.wrap(listSizeBytes);
                int listSize = listSizeBuffer.getInt();
                currentOffset += Integer.BYTES; // ข้าม List Size

                // 2. คำนวณและอ่าน Per-List Null Bitmap
                int perListNullBitmapBytes = (listSize + 7) / 8;
                byte[] perListNullBitmap = memory.readBytes(currentOffset, perListNullBitmapBytes);
                currentOffset += perListNullBitmapBytes;

                // 3. อ่าน Element ข้อมูลจริง (List)
                List<Value<?>> listValues = new ArrayList<>();
                ColumnMetaData.ColumnDType listDType = getMetaData().getDType();

                for (int j = 0; j < listSize; j++) {
                    boolean isElementNotNull = (perListNullBitmap[j / 8] & (1 << (j % 8))) != 0;

                    if (!isElementNotNull) {
                        listValues.add(new NullValue(new Object())); // Element เป็น Null
                        continue;
                    }

                    // Element เป็นแบบ Variable Length: [Size (4 bytes)][Data (N bytes)]

                    // 3a. อ่านขนาดของ Element (4 bytes)
                    byte[] elementSizeBytes = memory.readBytes(currentOffset, Integer.BYTES);
                    ByteBuffer elementSizeBuffer = ByteBuffer.wrap(elementSizeBytes);
                    int elementDataSize = elementSizeBuffer.getInt();
                    currentOffset += Integer.BYTES;

                    // 3b. อ่านข้อมูลจริง
                    byte[] elementBytes = memory.readBytes(currentOffset, elementDataSize);

                    // 3c. แปลงค่า DType
                    Value<?> elementValue;

                    // Note: สำหรับ Variable Length DType ที่สำคัญคือ String (LIST_STRING)
                    if (listDType.equals(ColumnMetaData.ColumnDType.LIST_STRING)) {
                        String s = new String(elementBytes);
                        elementValue = new Value<>(s);
                    } else {
                        // สำหรับ DType อื่นๆ หรือไม่รู้จัก ให้ใช้ byte[]
                        elementValue = new Value<>(elementBytes);
                    }

                    listValues.add(elementValue);
                    currentOffset += elementDataSize; // เลื่อน Offset ข้ามข้อมูลจริง
                }
                dataFrameCursor.setCursor(currentOffset);
                value = new Value<>(listValues); // คืนค่าเป็น Value ที่บรรจุ List<Value<?>>
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

        // 1. สร้าง Null Flags สำหรับรายการใหม่ตาม indexList
        boolean[] newNotNullFlags = new boolean[newRowCount];

        // อ่าน Bitmap ต้นฉบับครั้งเดียวเพื่อประสิทธิภาพ
        long srcBitmapSize = (this.rows + 7) / 8;
        byte[] srcBitmap = memory.readBytes((int) srcBitmapSize);

        for (int i = 0; i < newRowCount; i++) {
            int srcIdx = indexList.get(i);
            // ตรวจสอบ Null โดยใช้ Bitmap ที่อ่านมา
            boolean isNotNull = (srcBitmap[srcIdx / 8] & (1 << (srcIdx % 8))) != 0;
            newNotNullFlags[i] = isNotNull;
        }

        // 2. สร้าง Map ตำแหน่งข้อมูล (Offsets) เพื่อรองรับการเข้าถึงแบบ Random Access
        // เนื่องจาก Format เป็นแบบ Compact (Variable/List) เราต้อง Scan 1 รอบเพื่อหาจุดเริ่มต้นของแต่ละ Row
        long[] rowDataOffsets = new long[this.rows];

        if (columnKind != 0) { // ข้าม Fixed Scalar เพราะคำนวณ Offset ได้เลย
            long cursor = srcBitmapSize; // เริ่มต้นหลัง Bitmap

            for (int i = 0; i < this.rows; i++) {
                boolean isNotNull = (srcBitmap[i / 8] & (1 << (i % 8))) != 0;
                rowDataOffsets[i] = cursor; // บันทึกตำแหน่งเริ่มต้นของ Row i

                if (isNotNull) {
                    if (columnKind == 1) { // Variable Scalar
                        byte[] sizeBytes = memory.readBytes(cursor, Integer.BYTES);
                        int size = ByteBuffer.wrap(sizeBytes).getInt();
                        cursor += Integer.BYTES + size;
                    } else if (columnKind == 2) { // Fixed List
                        byte[] listSizeBytes = memory.readBytes(cursor, Integer.BYTES);
                        int listSize = ByteBuffer.wrap(listSizeBytes).getInt();
                        int bitmapBytes = (listSize + 7) / 8;
                        // ข้าม: Size + Bitmap + (Elements * ElementSize)
                        cursor += Integer.BYTES + bitmapBytes + ((long) listSize * sizePerElement);
                    } else if (columnKind == 3) { // Variable List
                        byte[] listSizeBytes = memory.readBytes(cursor, Integer.BYTES);
                        int listSize = ByteBuffer.wrap(listSizeBytes).getInt();
                        cursor += Integer.BYTES; // ข้าม List Size

                        int bitmapBytes = (listSize + 7) / 8;
                        byte[] listBitmap = memory.readBytes(cursor, bitmapBytes);
                        cursor += bitmapBytes; // ข้าม List Bitmap

                        // Scan elements ภายใน List เพื่อข้าม Data
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

        // 3. ดึงข้อมูลตาม indexList และสร้าง Column ใหม่
        switch (columnKind) {
            case 0: { // Fixed Scalar
                int totalSize = newRowCount * sizePerElement;
                ByteBuffer buffer = ByteBuffer.allocate(totalSize);

                for (int i = 0; i < newRowCount; i++) {
                    int srcIdx = indexList.get(i);
                    // คำนวณ Offset โดยตรง (สมมติว่า Fixed Scalar เก็บแบบ Direct Indexing หรือมีช่องว่างสำหรับ Null)
                    // ใช้ Logic เดียวกับ distributeColumn เดิม
                    long offset = srcBitmapSize + ((long) (this.startIdx + srcIdx) * sizePerElement);
                    byte[] data = memory.readBytes(offset, sizePerElement);
                    buffer.put(data);
                }
                buffer.flip(); // เตรียม Buffer สำหรับการอ่าน

                distributedColumn = new Column(metaData.getName(), sizePerElement, memoryGroupName, buffer, newNotNullFlags, sizePerElement, 0, newRowCount, dType);
                break;
            }
            case 1: { // Variable Scalar
                List<VariableElement> values = new ArrayList<>();
                for (int i = 0; i < newRowCount; i++) {
                    if (newNotNullFlags[i]) { // ถ้า Row ไม่เป็น Null
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
            case 2: { // Fixed List
                List<List<byte[]>> lists = new ArrayList<>();
                List<boolean[]> perListNotNullFlags = new ArrayList<>();

                for (int i = 0; i < newRowCount; i++) {
                    if (newNotNullFlags[i]) {
                        int srcIdx = indexList.get(i);
                        long offset = rowDataOffsets[srcIdx];

                        // Decode List Structure
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

                        // สร้าง Dense List ของ Elements (เฉพาะ Non-Null)
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
            case 3: { // Variable List
                List<List<VariableElement>> lists = new ArrayList<>();
                List<boolean[]> perListNotNullFlags = new ArrayList<>();

                for (int i = 0; i < newRowCount; i++) {
                    if (newNotNullFlags[i]) {
                        int srcIdx = indexList.get(i);
                        long offset = rowDataOffsets[srcIdx];

                        // Decode List Structure
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

                        // สร้าง Dense List ของ Variable Elements
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

        // 4. ตั้งค่า Metadata เพิ่มเติม
        if (distributedColumn != null) {
            distributedColumn.id = this.id;
            // startIdx สำหรับ Column ใหม่ที่เกิดจากการ Filter/Pick มักจะเริ่มที่ 0 หรือ logic เฉพาะ
            // ในที่นี้ Constructor ได้ตั้งเป็น 0 แล้ว แต่ต้อง update metadata อื่นๆ
            distributedColumn.metaData.setRows(newRowCount);
            distributedColumn.metaData.setSharded(true);
        }

        return distributedColumn;
    }




}
