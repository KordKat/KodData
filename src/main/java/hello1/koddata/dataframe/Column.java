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
            metaDataSize =
                    4 + metaData.getName().length() + // name length + data
                            1 + // isVariable (boolean)
                            Integer.BYTES + // rows
                            1; // isSharded (boolean)
        }

        // **เพิ่มขนาดสำหรับการซีเรียลไลซ์ Memory (peer และ allocatedSize)**
        long memoryDataSize = 0;
        if(memory != null){
            memoryDataSize = memory.size(); // ขนาดข้อมูลจริงที่ต้องคัดลอก
        }

        // คำนวณขนาด Buffer ที่รวมข้อมูล Memory
        int bufferSize = Long.BYTES + // id
                4 + memoryGroupName.length() + // memoryGroupName length + data
                metaDataSize +
                Byte.BYTES + // columnKind
                Integer.BYTES + // sizePerElement
                Integer.BYTES + // startIdx
                Integer.BYTES + // endIdx
                // **ส่วนของ Memory ที่เพิ่มมา:**
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
        }

        // 4. Serialize ข้อมูล Column ที่เหลือ
        buffer.put(columnKind);
        buffer.putInt(sizePerElement);
        buffer.putInt(startIdx);
        buffer.putInt(endIdx);

        // 5. **Serialize Memory Data**
        if (memory != null) {
            // allocatedSize (Peer Address ไม่ต้องซีเรียลไลซ์ เพราะมันเป็นแอดเดรสในเครื่องปัจจุบัน)
            buffer.putLong(memory.size());

            // ข้อมูลจริงจาก Off-heap Memory
            // เราใช้ memory.readBytes(int) เพื่อคัดลอกข้อมูลจาก Off-heap มาใส่ใน byte array
            byte[] rawData = memory.readBytes((int) memory.size());
            buffer.put(rawData);
        } else {
            // ถ้า memory เป็น null ให้ใส่ allocatedSize เป็น 0
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

        this.metaData = new ColumnMetaData(metaName, isVariable);

        // rows
        int rows = buffer.getInt();
        metaData.setRows(rows);

        // isSharded
        boolean isSharded = (buffer.get() == 1);
        metaData.setSharded(isSharded);

        // 3. Deserialize ข้อมูล Column ที่เหลือ
        this.columnKind = buffer.get();
        this.sizePerElement = buffer.getInt();
        this.startIdx = buffer.getInt();
        this.endIdx = buffer.getInt();

        // 4. **Deserialize และกู้คืน Memory Data**
        long allocatedSize = buffer.getLong();

        if (allocatedSize > 0) {

            // ดึงข้อมูลจริง (Raw Data)
            byte[] rawData = new byte[(int) allocatedSize];
            buffer.get(rawData);


            try {
                // ใช้ reflection เพื่อเข้าถึง MemoryUtil.unsafe
                Field f = Class.forName("hello1.koddata.memory.MemoryUtil").getDeclaredField("unsafe");
                f.setAccessible(true);
                sun.misc.Unsafe unsafe = (sun.misc.Unsafe) f.get(null);

                // 1. Allocate memory off-heap
                long peer = unsafe.allocateMemory(allocatedSize);

                // 2. Copy rawData to off-heap memory
                unsafe.copyMemory(rawData, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, peer, allocatedSize);


            } catch (Exception e) {

            }

        } else {
            this.memory = null;
        }
    }

    public Column distributeColumn(int startIdx, int endIdx){
        // สร้างอ็อบเจกต์ Column ใหม่
        Column distributedColumn = new Column();

        // ใช้อ้างอิง ID เดิม เพื่อชี้ไปยัง Memory เดียวกัน
        distributedColumn.id = this.id;

        // อ้างอิง Memory และ Memory Group Name เดิม
        distributedColumn.memoryGroupName = this.memoryGroupName;
        distributedColumn.memory = this.memory;

        // คัดลอก Metadata และ Properties อื่น ๆ
        // สร้าง ColumnMetaData ใหม่โดยใช้ Getter เพื่อคัดลอกค่า
        distributedColumn.metaData = new ColumnMetaData(this.metaData.getName(), this.metaData.isVariable());
        distributedColumn.metaData.setRows(endIdx - startIdx); // กำหนดจำนวนแถวสำหรับส่วนย่อยใหม่
        distributedColumn.metaData.setSharded(this.metaData.isSharded());

        distributedColumn.columnKind = this.columnKind;
        distributedColumn.sizePerElement = this.sizePerElement;

        // กำหนดช่วงดัชนีใหม่
        distributedColumn.startIdx = startIdx;
        distributedColumn.endIdx = endIdx;

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
        Value<?> value;
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
                byte[] size = memory.readBytes(offset, 4);

            }
            default -> value = null;
        }
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
}
